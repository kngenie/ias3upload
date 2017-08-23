#!/usr/bin/env perl
#
# ias3upload.pl - simple script for bulk-uploading to Internet Archive.
# Copyright (C) 2013  Kenji Nagahashi <kenji@archive.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
use strict;
use warnings;
#use FindBin;
#use lib "$FindBin::Bin/lib";
use LWP::UserAgent;
#use HTTP::Request::Common;
use HTTP::Date qw(str2time);
use URI::Escape;
use Getopt::Long;
use File::Spec;
use IO::File;
use Encode;
use English;

use constant IAS3URLBASE => 'http://s3.us.archive.org';
use constant IADLURLBASE => 'http://www.archive.org/download';
use constant IAMETAURLBASE => 'http://www.archive.org/metadata';
use constant META_XML => '_meta.xml';

use constant ENV_AUTHKEYS => 'IAS3KEYS';
use constant VERSION => '0.7.6';

use constant UPLOADJOURNAL => 'ias3upload.jnl';

my $inencoding = 'UTF-8';
my $outencoding = 'UTF-8';

sub resolvePath {
    my $rpath = shift;
    my $base = shift;
    # it seems I can't simply say File::Spec->rel2abs($rpath, $base).
    # File::Spec->rel2abs() requires directly as second argument.
    my $dir = File::Spec->catpath((File::Spec->splitpath($base))[0, 1]);
    return File::Spec->rel2abs($rpath, $dir);
}
sub readCSVRow {
    my $fh = shift;
    my $inquote = 0;
    my @fields = ();
    do {
	my $pos = $fh->getpos();
	defined (my $line = <$fh>) or return ();
	# if $line has CR in the middle, it is very likely CSV is saved in
	# Mac format (CR as newline). re read from $pos with EOL set to CR.
	if ($line =~ /\r(?!\n)/) {
	    $fh->setpos($pos) || die "ERROR:seek failed\n";
	    $/ = "\r";
	    $line = <$fh>;
	}
	my ($cols, $inquote2) = splitCSV($line, $inquote);
	if ($inquote) {
	    my $first = shift @{$cols};
	    $fields[$#fields] .= " $first" if $first ne '';
	}
	push(@fields, @{$cols});
	$inquote = $inquote2;
    } until (!$inquote);
    return @fields;
}

sub splitCSV {
    my $line = shift;
    my $inquote = (shift || 0);
    $line = decode($inencoding, $line);
    # chomp does not work well with CSV saved as "Windows CSV" on Mac.
    #chomp($line);
    $line =~ s/\s+$//;
    # following code handles quotes. we could use existing module
    # for doing this, but I wanted to keep this script 'stand-alone'.
    my @fields = ();
    my $l = $line;
    my @v;
    while ($l ne '') {
	if ($l =~ s/^""//) {
	    # escaped double-quote
	    push(@v, '"');
	} elsif ($l =~ s/^"//) {
	    $inquote = !$inquote;
	} elsif ($l =~ s/^,//) {
	    if ($inquote) {
		push(@v, ',');
	    } else {
		push(@fields, join('', @v));
		@v = ();
	    }
	} else {
	    my ($t) = $l =~ /^([^,"]*)/;
	    push(@v, $t);
	    $l = $POSTMATCH;
	}
    }
    push(@fields, join('', @v));
    return (\@fields, $inquote);
}

# obsoleted code that reads file content into a string of bytes.
# since this method won't work well with large files, I implemented
# PUT_FILE below for efficient file transfer.
sub getContent {
    my $path = shift;
    my $content;
    open(IN, $path) or die "failed to open $path: $!\n";
    my $o = 0;
    my $b;
    do {
	$b = read(IN, $content, 4096, $o);
	defined $b or die "error reading from $path: $!\n";
	$o += $b;
    } until ($b == 0);
    close(IN);
    print STDERR "$path: ", length($content), "bytes\n";
    return $content;
}

# variant of HTTP::Request::Common->PUT that handles upload of large file
# more efficiently. HTTP::Request::Common->POST has such feature built-in
# ($DYNAMIC_FILE_UPLOAD), but PUT doesn't. So we need a code for
# creating a custom PUT request. This also allows me to show progress of
# upload.
sub PUT_FILE {
    my $uri = shift;
    my $file = shift;
    my @headers = @_;

    if (defined $file) {
	my @st = stat($file) or die "can't stat $file:\n";
	my $size = $st[7];
	my $blocksize = $st[11] || 4096; # just in case it's zero...
	push(@headers, 'content-length', $size);
	# should we set content-type as well? probably CMS doesn't care.
	
	my $fh;
	my $sent = 0;
	my $content = sub {
	    # currently there's no safeguards against file's changing its
	    # size during transmission.
	    unless (ref($fh)) {
		open($fh, "<", $file) || die "can't open file $file:$!\n";
		binmode($fh);
	    }
	    # reading file in 2048 byte chunks.
	    my $buf;
	    my $n = read($fh, $buf, $blocksize, 0);
	    if ($n == 0) {
		close($fh);
	    } else {
		$sent += $n;
		my $pc = int($sent/$size*100);
		print STDERR "Sent $sent bytes ($pc%)\r";
	    }
	    return $buf;
	};
	my $request = HTTP::Request->new('PUT', $uri, \@headers, $content);
	return $request;
    } else {
	# no file to send
	push(@headers, 'content-length', 0);
	return HTTP::Request->new('PUT', $uri, \@headers, '');
    }
}

# escape non-printable chars to prevent those from confusing users
sub escapeText {
    $_[0] =~ s/[\x00-\x1f]/sprintf('\x%02x',ord($&))/ge;
    $_[0];
}

# simple-minded JSON parser. note $_[0] is modified.
sub parseJSON {
    no strict 'vars';
    local *json = \$_[0];
    $json =~ s/^\s+//;
    if ($json =~ s/^\{//) {
	my %d;
	unless ($json =~ s/^\s*}//) {
	    while (1) {
		# assumes no \-escape in keys
		$json =~ s/^\s*\"([^"]*)\"\s*:// or die "JSON key syntax error: $json";
		my $k = $1;
		my $v = parseJSON($json);
		$d{$k} = $v;
		last if $json =~ s/^\s*\}//;
		$json =~ s/^\s*,// or die "comma is expected: $json";
	    }
	}
	return \%d;
    } elsif ($json =~ s/^\s*\[//) {
	my @a;
	unless ($json =~ s/^\s*\]//) {
	    while (1) {
		my $v = parseJSON($json);
		push(@a, $v);
		last if $json =~ s/^\s*\]//;
		$json =~ s/^\s*,// or die "comma is expected: $json";
	    }
	}
	return \@a;
    } elsif ($json =~ s/^\"//) {
	my $v = "";
	while (1) {
	    $json =~ s/^[^\\"]*//;
	    $v .= $&;
	    if ($json =~ s/^\\u([0-9a-fA-F]{4})//) {
		$v .= chr(hex($1));
	    } elsif ($json =~ s/^\\x([0-9a-fA-F]{2})//) {
		$v .= chr(hex($1));
            } elsif ($json =~ s/^\\(.)//) {
		if ($1 eq 't') { $v .= "\t"; }
		else { $v .= $1; }
	    } else {
		$json =~ s/^"// or die "unterminated string at $json";
		last;
	    }
	}
	return $v;
    } elsif ($json =~ s/^[+-]?(\d+(\.\d*)?|\.\d+)//) {
	my $v = $&;
	return $v;
    } elsif ($json =~ s/^(null|false)//) {
	return undef;
    } elsif ($json =~ s/^true//) {
        return 1;
    } else {
	die "JSON syntax error: $json";
    }
}
sub fetchMetadata {
    my $ua = shift;
    my $itemname = shift;

    # retrieve item metadata with new metadata API.
    # API returns "{}" for non-existent item, rather than 404.
    my $res = $ua->get(IAMETAURLBASE.'/'.$itemname);
    if ($res->is_success) {
	# TODO JSON parse error handling
	my $json = $res->content;
	#print STDERR "META:".$json."\n";
	my $data = parseJSON($json);
	unless (defined $data) {
	    # item does not exist - this is not an error.
	    return {server=>undef, dir=>undef,
		    files=>undef, created=>undef, metadata=>{}};
	}
	return $data;
    } else {
	return undef;
    }
}

my $ias3keys;

# controls
my $help_and_exit = 0;
my $initConfig = 0;
# no actual upload
my $dryrun = 0;
# print more info
my $verbose = 0;
# ignore previous upload
my $forceupload = 0;
# check against storage (slow)
my $checkstore = 0;
my $metatbl = 'metadata.csv';	# CSV file having metadata for each item

# default metadata from command line
my %metadefaults;

# how metadata is applied to items:
# 'keep': make no change to existing metadata.
# 'update': keep existing metadata (if item exists), update those specified in
#   metadata.csv.
# 'replace': wipe out existing metadata and set what's specified in metadata.csv
#   anew.
my $metadataAction = 'update';
# don't update metadata of existing items (in fact it is the default
# behavior of IAS3. I made 'override-mode' default because it matches user's
# expectation.)
#my $keepExistingMetadata = 0;
my $noDerive = 0;
#my $forceMetadataUpdate = 0;
my $ignoreNofile = 0;
# these control options are not implemented yet.
my $keepOldVersion = 0;
my $cascadeDelete = 0;

my $homedir = $ENV{'HOME'};
$homedir =~ s![^/]$!$&/!;	# ensure $homedir has trailing slash

sub confirm {
    my ($prompt, $emptyRes) = @_;
    while (1) {
	print $prompt;
	chomp(my $ans = <STDIN>);
	return $emptyRes if defined $emptyRes and $ans eq '';
	return 1 if $ans =~ /^y(|es|a|eah)$/i;
	return 0 if $ans =~ /^n(|o|ope)$/i;
	print "Please answer y or n\n";
    }
}

sub initConfig {
    $homedir || die "sorry, failed to locate your home directory\n";
    -d $homedir || die "your home directory does not exist...?\n";

    STDOUT->autoflush(1);
    my $cfg = $homedir . ".ias3cfg";
    my $ans;
    warn "\nI'm going to create ~/.ias3cfg with your IAS3 keys for you.\n";
    if (-f $cfg) {
	unless (confirm("\nOh, you already have ~/.ias3cfg. Are you sure you want to overwrite it? (y/N):", 0)) {
	    die "Alright, quitting.\n";
	}
    }
    my ($accessKey, $secretKey);
    if (confirm("\nI can get your keys from IA web site for you. Want to try? (y/N):", 0)) {
	require Term::ReadKey;
	my ($username, $password);
	{
	    print "\nEnter your Internet Archive account email address: ";
	    Term::ReadKey::ReadMode(1);
	    chomp($username = <STDIN>);
	    Term::ReadKey::ReadMode(2);
	    print "Enter password (you'll not see your keystrokes): ";
	    chomp($password = <STDIN>);
	    Term::ReadKey::ReadMode(0);
	    ($accessKey, $secretKey) = getKeysFromWeb($username, $password);
	    redo if $accessKey eq 'login failed';
	}
    }
    unless ($accessKey && $secretKey) {
	print "\nPlease have your IAS3 keys ready. If you don't have them yet, ",
	"get them from http://www.archive.org/account/s3.php.\n\n";
	{
	    print "Enter your IAS3 access key: ";
	    chomp($accessKey = <STDIN>);
	    unless (checkKey($accessKey)) {
		print "Bad key - it should be letters and digits, length 16.\n";
		redo;
	    }
	}
	{
	    print "Enter your IAS3 secret key: ";
	    chomp($secretKey = <STDIN>);
	    unless (checkKey($secretKey)) {
		print "Bad key - it should be letters and digits, length 16.\n";
		redo;
	    }
	}
    }
    writeConfig($cfg, $accessKey, $secretKey);
    STDOUT->autoflush(0);
}

sub writeConfig {
    my ($cfg, $accessKey, $secretKey) = @_;
    STDOUT->printflush("Writing ~/.ias3cfg...");
    unless (open(CF, '>', $cfg)) {
	die "\noops, failed to open $cfg for writing: $!\n";
    }
    print CF "access_key = $accessKey\n";
    print CF "secret_key = $secretKey\n";
    close(CF);
    # make sure it's only readable by the owner
    chmod(0600, $cfg);
    print "done.\n";
}

sub checkKey {
    $_[0] =~ /^[[:alnum:]]{16}$/;
}

sub getKeysFromWeb {
    my ($username, $password) = @_;
    my $ua = LWP::UserAgent->new;
    $ua->agent('ias3upload/' . VERSION);
    $ua->timeout(20);
    $ua->env_proxy;

    require HTTP::Cookies;
    $ua->cookie_jar(HTTP::Cookies->new());
    my $res;
    print "\nLogging in...\n";
    $ua->get('http://www.archive.org/account/login.php');
    $res = $ua->post('http://www.archive.org/account/login.php',
		     { username => $username, password => $password,
		       remember => 'CHECKED', submit => 1 });
    my $loginpage = $res->content;
    if ($loginpage =~ /Error: Invalid password or username/s) {
	warn "Login failed. Please try again.\n";
	return ('login failed');
    }
    warn "\nRetrieving S3 keys www.archive.org...\n";
    $res = $ua->get('http://www.archive.org/account/s3.php');
    if ($res->is_success) {
	my $content = $res->content;
	my @keys = $content =~ /Your S3 access key: ([[:alnum:]]{16}).*Your S3 secret key: ([[:alnum:]]{16})/s;
	if (@keys) {
	    warn "okay, found keys successfully.\n";
	} else {
	    warn "sorry, couldn't find keys on the page, probably due to page layout change. Please get keys manually.\n";
	}
	return @keys;
    } else {
	warn "sorry, couldn't load S3 keys page (possibly problem with network or Internet Archive server). Please get keys manually.\n";
	return ();
    }
}
    
sub readConfig {
    my $name = shift;
    open(CF, $name) || return;
    print STDERR "reading configuration from $name...\n";
    my @keys = ('', '');
    # assumes one-parameter-per-line format of .s3cfg (s3cmd)
    while (<CF>) {
	next unless /^([_a-zA-Z]+)\s*=\s*(.*?)\s*$/;
	my $param = $1;
	my $value = $2;
	$param eq 'access_key' and ($keys[0] = $value, next);
	$param eq 'secret_key' and ($keys[1] = $value, next);
    }
    close(CF);
    if ($keys[0] && $keys[1]) {
	$ias3keys = join(':', @keys);
    }
}

sub encodeHeaderValue {
    my $v = shift;
    # replace vertical TAB(0x0b) with NL
    $v =~ s/\x0b/\x0a/g;
    # if value contains non-printable, use uri() encoding offered by IAS3
    if ($v =~ /[\x00-\x1f]/) {
	$v = encode($outencoding, $v);
	$v =~ s/(\W)/'%'.unpack('H2',$1)/eg;
	return 'uri('.$v.')';
    } else {
	return encode($outencoding, $v);
    }
}
sub metadataHeaders {
    my ($h, $v) = @_;
    # Since RFC822 disallow '-' in header names, IAS3 translates
    # '--' to '_'. Need to leverage that 'escaping' here (nothing difficult)
    $h =~ s/_/--/g;
    if (ref $v eq 'ARRAY') {
	if ($#{$v} == 0) {
	    # if there's only one value, we don't use indexed form
	    return ('x-archive-meta-' . $h, encodeHeaderValue($v->[0]));
	} else {
	    my $i = 1;
	    return map((sprintf('x-archive-meta%02d-%s', $i++, $h),
			encodeHeaderValue($_)),
		       @$v);
	}
    } else {
	return ('x-archive-meta-' . $h, encodeHeaderValue($v));
    }
}

# is this the last file to upload in its item?
sub lastFile {
    my $file = shift;
    my $item = $file->{item};
    my $c = 0;
    foreach my $f (@{$item->{files}}) {
	$c++ if $f->{upload};
    }
    print STDERR "$c file(s) to upload in ", $item->{name}, "\n";
    return $c == 1;
}
# test for metadata
sub unspecified {
    my $v = shift;
    return !defined($v) || $v eq '' || ref $v eq 'ARRAY' && $#$v == -1;
}
# IAS3 auth keys are taken from three locations
# (from lowest to highest priority):
# 1) {access,secret}_key parameters in $HOME/.s3cfg
# 2) {access,secret}_key parameters in $HOME/.ias3cfg
# 3) IAS3KEYS environment variable
# 4) -k command line option

if ($homedir) {
    readConfig($homedir . ".s3cfg"); # config file for s3cmd
    readConfig($homedir . ".ias3cfg"); # IAS3 config file, in the same format
}

if (exists $ENV{ENV_AUTHKEYS()}) {
    my $keys = $ENV{ENV_AUTHKEYS()};
    unless ($keys =~ /^[A-Za-z0-9]+:[A-Za-z0-9]+$/) {
	warn "WARNING:", ENV_AUTHKEYS, " should be in format ACCESSKEY:SECRETKEY (ignored)\n";
    } else {
	$ias3keys = $keys;
    }
}

GetOptions('h'=>\$help_and_exit,
	   'n'=>\$dryrun,
	   'v+'=>\$verbose,
	   'f'=>\$forceupload,
	   'm'=>\$checkstore,
	   'l=s'=>\$metatbl,
	   'k=s'=>\$ias3keys,
	   'c=s'=>\$metadefaults{'collection'},
	   'i=s'=>\$metadefaults{'item'},
	   # not yet supported metadata default options
	   'dd=s'=>\$metadefaults{'description'},
	   'dc=s'=>\$metadefaults{'creator'},
	   'dm=s'=>\$metadefaults{'mediatype'},
	   'init'=>\$initConfig,
	   # control options
	   'keep-metadata'=>sub { $metadataAction = 'keep'; },
	   'replace-metadata'=>sub { $metadataAction = 'replace'; },
	   'no-derive'=>\$noDerive,
	   'ignore-nofile'=>\$ignoreNofile,
	   #'update-metadata'=>\$forceMetadataUpdate,
	   # not yet supported control options
	   'keep-old'=>\$keepOldVersion,
	   'cascade-delete'=>\$cascadeDelete,
    );
if ($help_and_exit) {
    print STDERR <<"EOH";
$0 [OPTIONS]
options:
    -h \tshow this help message and exit.
    -l METADATA.CSV use specified file as upload description 
      \t\t(default ./metadata.csv)
    -n\t\tsimulate upload process, but don't actually upload files.
    -f\t\tupload all files ignoring upload history.
    -m\t\tquery storage server to confirm the file being uploaded is
      \t\tin fact a new file (can be slow).
    -c COLLECTIONS\tdefault collection.
    -i ITEMID\tdefault item name.
    --keep-metadata\tkeep existing metadata (metadata ignored for existing items).
    --replace-metadata\treplace entire metadata with what's given.
    --no-derive\tdo not trigger derive.
    --init\tcreate ~/.ias3cfg file by fetching credentials from IA web.
    -v\t\tprint extra trace output.
EOH
    exit(0);
}
# check for incompatible option combinations
#if ($keepExistingMetadata && $forceMetadataUpdate) {
#    die "conflicting options: --update-metadata and ".
#	"--keep-existing-metadata";
#}
if ($initConfig) {
    initConfig();
    exit(0);
}

unless (defined $ias3keys) {
    die "ERROR:".
	"I need your IAS3 key pair for calling IAS3 API. ".
	"Please supply it by one of following methods:\n\n".
	"1) add command line option \"-k <access_key>:<secret_key>\",\n".
	"2) set <access_key>:<secret_key> to environment variable '".ENV_AUTHKEYS."',\n".
	"3) create a file '.ias3cfg' in your home directory with your access_key and\n".
	"   secret_key parameters in it (run '$0 --init' to create it\n".
	"   interactively)\n\n".
	"You can get your IAS3 keys at http://www.archive.org/account/s3.php (login required)\n";
}
unless ($ias3keys =~ /^[A-Za-z0-9]+:[A-Za-z0-9]+$/) {
    die "ERROR:keys must be in format ACCESSKEY:SECRETKEY\n";
}
# process multi-valued metadata defaults
foreach my $m (('collection')) {
    if (defined $metadefaults{$m}) {
	my @values = split(/\s*[,;]\s*/, $metadefaults{$m});
	$metadefaults{$m} = \@values;
    }
}

# entire metatbl is read into memory before starting upload.
my $mt = new IO::File;
unless ($mt->open($metatbl)) {
    die "cannot open $metatbl: $!\n";
}
my $task = {
    items => {},
    files => []
};
# keep change to $/ local
{
local($/) = $/;    
my @fieldnames = readCSVRow($mt);
my %colidx;
foreach my $i (0..$#fieldnames) {
    print STDERR "Field[", $i + 1, "]:", $fieldnames[$i], "\n" if $verbose;
    # just ignore empty metadata name cell without complaining
    next if $fieldnames[$i] eq '';
    unless ($fieldnames[$i] =~ /^([-a-zA-Z_]+)(\[\d+\])?$/) {
	die "ERROR:bad metadata name '", escapeText($fieldnames[$i]), "' in column ".($i + 1)."\n";
    }
    my $fn = $1;
    my $ix = $2;
    # index part is unused for now and simply discarded
    $fieldnames[$i] = $fn;
    push(@{$colidx{$fn}}, $i);
}
# some must-have fields
# "file" field must exist as a column - no command line default
exists $colidx{'file'} or die "ERROR:required column 'file' is missing\n";
# "item" may be given in a column or in command line
foreach my $cn (('item')) {
    unless (exists $colidx{$cn} || defined $metadefaults{$cn}) {
	die "ERROR:'$cn' must either exist as a column, or be specified by option\n";
    }
}
# # other metadata fields may be given in a column or in command line
# foreach my $cn (('item', 'creator', 'mediatype', 'collection')) {
#     unless (exists $colidx{$cn} || defined $metadefaults{$cn}) {
# 	die "ERROR:'$cn' must either exist as a column, or be specified by option\n";
#     }
# }
# check for columns that can appear only once
foreach my $cn (('item', 'file', 'mediatype')) {
    if (exists $colidx{$cn} && $#{$colidx{$cn}} > 0) {
	die "ERROR:sorry, you can't have $cn in more than one column\n";
    }
    $colidx{$cn} = $colidx{$cn}->[0] if exists $colidx{$cn};
}

my $curCollections = [];
my $curItem;
# read on rest of the metatbl file... we first read entire metatbl file
# to construct a list of tasks to be performed (TODO merge in information
# from jornal of previous upload for retry/update). Verify information to
# report any errors before starting actual upload work.

# collections named in command-line become initial $curCollections
if ($metadefaults{'collection'}) {
    # note $metadefaults{'collection'} is an array if defined
    $curCollections = $metadefaults{'collection'};
}
# similarly for $curItem
if ($metadefaults{'item'}) {
    $curItem = { name => $metadefaults{'item'},
		 metadata => {},
		 files => [] };
    $task->{items}{$curItem->{name}} = $curItem;
}

# read body rows
while (my @fields = readCSVRow($mt)) {
    # skip empty row
    next unless (grep(/\S/, @fields));
    my $collections = [];
    if (defined $colidx{'collection'}) {
	my @collections = grep($_, @fields[@{$colidx{'collection'}}]);
	$collections = \@collections;
    }
    # $curCollections carries over only when 'collection' columns are all empty
    unless (@$collections) { $collections = $curCollections; }
#     @$collections or (@collections = @$curCollections);
#     unless (@collections) {
# 	die "ERROR:collection is unknown at $metatbl:$.";
#     }

    my $itemName = (defined $colidx{'item'}) && $fields[$colidx{'item'}]
	|| $curItem->{name};
    unless ($itemName) {
	die "item identifier is unknown at $metatbl:$.\n";
    }
    my $item = ($task->{items}{$itemName}
		||= { name=>$itemName, metadata=>{}, files=>[] });
    
    $item->{metadata}{collection} = $collections;
    #$item->{collections} = \@collections;

    # allow for a row without "file" (i.e. empty), which just specifies
    # metadata for the item, no file to upload
    my $file = (exists $colidx{'file'}) && $fields[$colidx{'file'}];
    if ($file) {
	# file field designate a file relative to metatbl.
	my $path = resolvePath($file, $metatbl);
	# filename is used as the name of uploaded file (last component of URL)
	# XXX: currently ignores directory part - when multiple files in an item
	# have the same filename, last one clobbers previous ones.
	# @pathcomps = (volume, directory, filename)
	my @pathcomps = File::Spec->splitpath($file);
	my $filename = $pathcomps[2];
	# do some sanity check on the file now.
	unless (-e $path) {
	    die "$path: file does not exist\n";
	}
	unless (-f _) {
	    die "$path: not a plain file\n";
	}
	unless (-r _) {
	    die "file $path is not readable\n";
	}
	my @st = stat(_);
	unless (@st) {
	    die "stat failed on $path: $!\n";
	}
	my $fileobj = {
	    file=>$file, path=>$path, filename=>$filename,
	    item=>$item, size=>$st[7], mtime=>$st[9]
	};
	push(@{$item->{files}}, $fileobj);
    }

    # add metadata to item. As currently only items get metadata, it's simple.
    # it will become confusing when backend start accepting metadata for
    # files -- which we should apply metadata, file or item, for rows where
    # they are created at once?
    foreach my $i (0..$#fields) {
	# $fieldnames[$i] is 'undefined' for empty header, but $fn will be
	# empty string, not 'undefined'.
	my $fn = $fieldnames[$i];
	if ($fn eq '' && $fields[$i] ne '') {
	    warn "WARNING:$metatbl:$.:",
	    "a value found in column ", $i + 1, ", which has no metadata name ",
	    "(ignored)\n";
	    next;
	}
	# these fields are special and already handled above
	next if $fn =~ /^(file|item|collection)$/;
	# other fields are plain metadata (X-Archive-Meta-* headers)
	# note that index ([\d+] after column name) doesn't matter at all
	# and multiple metadata are sent in the order they appear in a row
	# (also metadata index would be different from those user specified in
	# metadata.csv). we'd need to change this behavior if users want to
	# enforce order with indexes.
	push(@{$item->{metadata}{$fn}}, $fields[$i])
	    if $fields[$i] ne '';
    }
		
    # use item identifier as title if unspecified
    $item->{'title'} ||= $item->{name};

    $curItem = $item;
    $curCollections = $collections;

}
} # end of scope for $/
$mt->close();

# calculate total upload size for each item, for size-hint
foreach my $item (values %{$task->{items}}) {
    foreach my $file (@{$item->{items}}) {
	$item->{size} += $file->{size};
    }
}

# read journal file left by previous upload
my $journalFile = resolvePath(UPLOADJOURNAL, $metatbl);
if (open(my $rjnl, '<', $journalFile)) {
    my %fileidx;
    foreach my $item (values %{$task->{items}}) {
	foreach my $file (@{$item->{files}}) {
	    $fileidx{$file->{file}} = $file;
	}
    }
    while ($_ = <$rjnl>) {
	chomp;
	if (/^U (.*)/) {
	    my ($file, $mtime, $itemName, $filename) = split(/\s+/, $1);
	    $file = uri_unescape($file);
	    $filename = uri_unescape($filename);
	    if (exists $fileidx{$file}) {
		$fileidx{$file}->{uploaded} = {
		    mtime => $mtime,
		    itemName => $itemName,
		    filename => $filename
		};
	    }
	}
    }
    close($rjnl);
}

my $ua = LWP::UserAgent->new();
$ua->agent('ias3upload/' . VERSION);
$ua->timeout(20);
$ua->env_proxy;

$ua->default_headers->push_header('authorization'=>"LOW $ias3keys");
#$ua->default_headers->push_header('x-amz-auto-make-bucket'=>'1');

# collect files to upload
foreach my $item (values %{$task->{items}}) {
    foreach my $file (@{$item->{files}}) {
	my $uripath = "/" . $file->{item}{name} . "/" . $file->{filename};
	if (!$forceupload) {
	    if (my $last = $file->{uploaded}) {
		# this file was uploaded in previous run. re-upload it only when
		# something has changed.
		if ($file->{mtime} <= $last->{mtime} &&
		    $file->{item}{name} eq $last->{itemName} &&
		    $file->{filename} eq $last->{filename}) {
		    warn "File: ", $file->{file},
		    ": skipping - no change since last upload\n";
		    next;
		}
	    }
	}
	if ($checkstore) {
	    my $dlurl = IADLURLBASE . $uripath;
	    print STDERR "checking ", $dlurl, "...\n" if $verbose;
	    my $res = $ua->head($dlurl);
	    if ($res->is_success) {
		# file exists - check date (of last upload) against file's mtime
		my $m = $res->headers->{'date'};
		if ($m && str2time($m) >= $file->{mtime}) {
		    warn "skipping - upload date later than file's mtime\n";
		    next;
		}
	    } else {
		# 404 or other failure - upload the file
		print $res->status_line, "\n";
	    }
	}
	$file->{upload} = 1;
	push(@{$task->{files}}, $file);
    }
    # if metadata update is requested, schedule a dummy file for items
    # with zero files to upload.
    if (!$ignoreNofile && !grep($_->{upload}, @{$item->{files}})) {
	my $dummyfile = { filename=>'*_meta.xml', item=>$item };
	push(@{$task->{files}}, $dummyfile);
    }
}
    
# then open journal file for writing (append mode).
open(my $jnl, '>>', $journalFile) or
    die "cannot open a journal file: $journalFile:$!\n";

# now start actual upload tasks, doing some optimization.
# - items with no file to upload are not created
# - item creation is always combined with the first file upload
my @uploadQueue = @{$task->{files}};
while (@uploadQueue) {
    my $file = shift @uploadQueue;
    my $uripath;
    # file object without 'file' member instructs forced metadata update.
    if (!(defined $file->{file})) {
	$uripath = "/" . $file->{item}{name};
	warn "Item: ", $uripath, "\n";
    } else {
	$uripath = "/" . $file->{item}{name} . "/" . $file->{filename};
	warn "File: ", $file->{file}, " -> ", $uripath, "\n";
    }
    my $waitUntil = $file->{waitUntil};
    if (defined $waitUntil) {
	my $sec = $waitUntil - time();
	while ($sec > 0) {
	    print STDERR "holding off $sec second", ($sec > 1 ? 's' : ''), "...   ";
	    sleep(1);
	    $sec--;
	} continue { print STDERR "\r"; }
	print STDERR "\n";
	delete $file->{waitUntil};
    }
    # ok, ready to go
    my $item = $file->{item};
    my @headers = ();
    # prepare item metadata if the item hasn't been created yet (in this
    # session) - it might exist on the server.
    unless ($item->{created}) {
	my $metadata = $item->{metadata};
	if ($metadataAction eq 'update') {
            print STDERR "retrieving existing metadata for $item->{name}...\n"
            if $verbose;
	    my $exmetadata = fetchMetadata($ua, $item->{name});
	    # crucial metadata may be lost if we proceed without fetching metadata
	    unless (defined $exmetadata) {
		warn "failed to get metadata of item $item->{name}\n";
		$file->{waitUntil} = time() + 120;
		push(@uploadQueue, $file);
		next;
	    }
            unless ($exmetadata->{server}) {
		print STDERR "item $item->{name} does not exist yet.\n" if $verbose;
	    }
	    $exmetadata = $exmetadata->{metadata};
	    for my $k (('identifier')) {
		delete $exmetadata->{$k};
	    }
	    for my $k (keys %$exmetadata) {
		if (unspecified($metadata->{$k})) {
		    $metadata->{$k} = $exmetadata->{$k};
		    print STDERR "existing metadata %k=".$exmetadata->{$k}
		    if $verbose > 1;
		}
	    }
	}
	# check metadata
	my @metaerrs = ();
	for my $k (('mediatype', 'collection')) {
	    push(@metaerrs, $k) if unspecified($metadata->{$k});
	}
	if (@metaerrs) {
	    die "ERROR:following mandatory metadata is undefined for item '"
		.$item->{name}."':\n".join('', map("  $_\n", @metaerrs));
	}
	# prepare actual HTTP headers for metadata
	push(@headers, 'x-amz-auto-make-bucket', 1);
	# As metadata (most often 'collection' and 'subject') may have multiple
	# values, %metadata has an array for each metadata name (in some cases,
	# notably 'title', may be a scalar). If there in fact are multiple values,
	# we use metadata header in indexed form. If there's only one value
	# (either in an array or as a scalar), we use basic form. Special metadata
	# 'collection' is also handled by this same logic.
	while (my ($h, $v) = each %$metadata) {
	    push(@headers, metadataHeaders($h, $v));
	}
	# add metadata headers for collections item gets associated with
	#my @collectionNames = map($_->{name}, @{$item->{collections}});
	#push(@headers, metadataHeaders('collection', \@collectionNames));

	# overwrite existing bucket unless user explicitly told not to.
	unless ($metadataAction eq 'keep') {
	    push(@headers, 'x-archive-ignore-preexisting-bucket', '1');
	}

	# size-hint
	if ($item->{size}) {
	    push(@headers, 'x-archive-size-hint', $item->{size});
	}
    }
    # to reduce the workload on IA catalogue system,
    # all file upload should have no-derive but the last one. if no-derive
    # is specified, it goes with the last one, too.
    # no-derive flag should go with all files
    if ($noDerive || !lastFile($file)) {
	push(@headers, 'x-archive-queue-derive', '0');
    }
    # Expect header
    push(@headers, 'Expect', '100-continue');

    my $uri = IAS3URLBASE . $uripath;
    my $content = $file->{path};
    
    if ($verbose) {
	print STDERR "PUT $uri\n";
	for (my $i = 0; $i < $#headers; $i += 2) {
	    print STDERR $headers[$i], ":", $headers[$i + 1], "\n";
	}
    }

    if ($dryrun) {
	print STDERR "## dry-run; not making actual request\n";
	$file->{upload} = 0;
    } else {
	# use of custom PUT_FILE is for efficient handling of large files.
	# see comment on PUT_FILE above.
	my $req = PUT_FILE $uri, $content, @headers;
	#print STDERR $req->as_string;
	my $res = $ua->request($req);
	print STDERR "\n";
	if ($res->is_success) {
	    $file->{upload} = 0;
	    print $res->status_line, "\n";
	    $res->headers->scan(sub { print "$_[0]: $_[1]\n"; }) if $verbose;
	    print $res->content, "\n" if $verbose;
	    printf($jnl "U %s %s %s %s\n",
		   uri_escape_utf8($file->{file}),
		   $file->{mtime}, $file->{item}{name},
		   uri_escape_utf8($file->{filename}))
		if $file->{file};
	    $jnl->flush();
	    print "\n";
	} else {
	    print $res->status_line, "\n", $res->content, "\n\n";
	    if ($res->code == 503) {
		# Service Unavailable - asking to slow down
		$file->{waitUntil} = time() + 120;
		# put it at the head so that it blocks transfer
		unshift(@uploadQueue, $file);
	    } elsif (++$file->{failCount} < 5) {
		$file->{waitUntil} = time() + 120;
		push(@uploadQueue, $file);
	    } else {
		# give up
	    }
	    next;
	}
    }
    
    $item->{created} = 1;
}

close($jnl);
