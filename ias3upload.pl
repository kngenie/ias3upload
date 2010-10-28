#!/usr/bin/perl
#
use strict;
use warnings;
#use FindBin;
#use lib "$FindBin::Bin/lib";
use LWP::UserAgent;
#use HTTP::Request::Common;
use Getopt::Long;
use File::Spec;
use IO::Handle;

use constant IAS3URLBASE => 'http://s3.us.archive.org/';
use constant ENV_AUTHKEYS => 'IAS3KEYS';
use constant VERSION => '0.5.1';

sub splitCSV {
    my $line = shift;
    # chomp does not work well with CSV saved as "Windows CSV" on Mac.
    #chomp($line);
    $line =~ s/\s+$//;
    # following code handles quotes. we could use existing module
    # for doing this, but I wanted to keep this script 'stand-alone'.
    my @pieces = split(/([,"])/, $line);
    my @fields = ();
    my @v;
    while (@pieces) {
	my $t = shift @pieces;
	if ($t eq ',') {
	    push(@fields, join('', @v));
	    @v = ()
	} elsif ($t eq '"') {
	    $t = shift @pieces;
	    while (defined $t && $t ne '"') {
		push(@v, $t);
		$t = shift @pieces;
	    }
	} else {
	    push(@v, $t);
	}
    }
    push(@fields, join('', @v));
    return @fields;
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
	    my $n = read($fh, $buf, 2048, 0);
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
    $_[0] =~ s/[\x00-\x1f]/xsprintf('\x%02x',$&)/ge;
    $_[0];
}

my $ias3keys;

# controls
my $initConfig = 0;
my $dryrun = 0;
my $verbose = 0;
my $metatbl = 'metadata.csv';	# CSV file having metadata for each item

# default metadata from command line
my %metadefaults;

# don't update metadata of existing items (in fact it is the default
# behavior of IAS3. I made 'override-mode' default because it matches user's
# expectation.)
my $keepExistingMetadata = 0;
# these control options are not implemented yet.
my $keepOldVersion = 0;
my $cascadeDelete = 0;
my $noDerive = 0;

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

GetOptions('n'=>\$dryrun,
	   'v'=>\$verbose,
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
	   'keep-existing-metadata'=>\$keepExistingMetadata,
	   # not yet supported control options
	   'keep-old'=>\$keepOldVersion,
	   'cascade-delete'=>\$cascadeDelete,
	   'no-derive'=>\$noDerive,
    );

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
	my @values = split(/\s*,\s*/, $metadefaults{$m});
	$metadefaults{$m} = \@values;
    }
}

# metatbl is kept open until the end of all uploads.
unless (open(MT, $metatbl)) {
    die "cannot open $metatbl: $!\n";
}
my $specline = <MT>;
# if $specline has CR in the middle, it is very likely CSV is saved in
# Mac format. start over with EOL set to CR.
if ($specline =~ /\r[^\n]/) {
    seek(MT, 0, 0) || die "ERROR:seek on $metatbl failed\n";
    $/ = "\r";
    $specline = <MT>;
}
my @fieldnames = splitCSV($specline);
my %colidx;
foreach my $i (0..$#fieldnames) {
    print STDERR "Field:", $fieldnames[$i], "\n" if $verbose;
    unless ($fieldnames[$i] =~ /^([-a-zA-Z]+)(\[\d+\])?$/) {
	die "ERROR:bad metadata name ", escapeText($fieldnames[$i]), " in column ".($i + 1)."\n";
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
# other metadata fields may be given in a column or in command line
foreach my $cn (('item', 'creator', 'mediatype', 'collection')) {
    unless (exists $colidx{$cn} || defined $metadefaults{$cn}) {
	die "ERROR:'$cn' must either exist as a column, or be specified by option\n";
    }
}
# check for columns that can appear only once
foreach my $cn (('item', 'file', 'mediatype')) {
    if (exists $colidx{$cn} && $#{$colidx{$cn}} > 0) {
	die "ERROR:sorry, you can't have $cn in more than one column\n";
    }
    $colidx{$cn} = $colidx{$cn}->[0];
}

my $ua = LWP::UserAgent->new();
$ua->agent('ias3upload/' . VERSION);
$ua->timeout(20);
$ua->env_proxy;

$ua->default_headers->push_header('authorization'=>"LOW $ias3keys");
$ua->default_headers->push_header('x-amz-auto-make-bucket'=>'1');

my $curCollection;
my $curItem;
# read on the metatbl file...
# TODO: probably we should scan entire metatbl first to get rough idea of
# how big each item would be. It would allow us to give 'size-hint' header
# to IA CMS.
while (<MT>) {
    my @fields = splitCSV($_);
    my %headers;
    foreach my $i (0..$#fields) {
	my $fn = $fieldnames[$i];
	if (!defined($fn) && $fields[$i] ne '') {
	    warn "WARNING:$metatbl:$.:",
	    "a value found in column $i without metadata name (ignored)\n";
	    next;
	}
	# these fields requires special handling
	next if $fn =~ /^file|item|collection$/;
	# other fields are plain metadata (X-Archive-Meta-* headers)
	# note that index ([\d+] after column name) doesn't matter at all
	# and multiple metadata are sent in the order they appear in a row
	# (also metadata index would be different from those user specified in
	# metadata.csv). we'd need to change this behavior if users want to
	# enforce order with indexes.
	push(@{$headers{$fn}}, $fields[$i])
	    if $fields[$i] ne '';
    }
    # request URI
    my $uri;
    # content (actually name of a file containing it) to be sent
    my $content;

    my $collection = (exists $colidx{'collection'}
		      && [grep($_, @fields[@{$colidx{'collection'}}])]);
    # $curCollection is used only when all 'collection' columns are empty.
    if (!$collection || $#{$collection} == -1) {
	$collection = $curCollection || $metadefaults{'collection'};
    }
    unless ($collection && $#{$collection} >= 0) {
	die "collection is unknown at ";
    }
    $headers{'collection'} = $collection;

    my $item = (exists $colidx{'item'} && $fields[$colidx{'item'}])
	|| $curItem || $metadefaults{'item'};
    unless ($item) {
	die "item is unknown at ";
    }
    unless ($headers{'title'}) {
	$headers{'title'} = [$item];
    }

    # probably I should allow for a row without "file", which just specifies
    # metadata for the item.
    my $file = $fields[$colidx{'file'}];
    if ($file) {
	# file field is relative to metatbl. It seems I can't simply say
	# $path = File::Spec->rel2abs($file, $metatbl);
	my $metatbldir = File::Spec->catpath((File::Spec->splitpath($metatbl))[0,1]);
	my $path = File::Spec->rel2abs($file, $metatbldir);
	# filename is used as the name of uploaded file (last component of URL)
	# XXX: currently ignores directory part - when multiple files within an item
	# have the same filename, last one clobbers previous ones.
	# @pathcomps = (volume, directory, filename)
	my @pathcomps = File::Spec->splitpath($file);
	my $filename = $pathcomps[2];
	$uri = IAS3URLBASE."$item/$filename";
	$content = $path;
	print STDERR "Uploading $path\n";
    } else {
	# creating an item
	$uri = IAS3URLBASE.$item;
	# leave $content undef
    }

    my @headers = ();
    # As metadata (most often 'collection' and 'subject') may have multiple
    # values, %headers has an array for each metadata name (in some case,
    # notably 'title' may be a scalar). If there in fact multiple values,
    # we use metadata header in indexed form. If there's only one value,
    # we use basic form. Special metadta collection is also handled by
    # this logic.
    while (my ($h, $v) = each %headers) {
	if (ref $v eq 'ARRAY') {
	    if ($#{$v} == 0) {
		# if there's only one value, we don't use indexed form
		push(@headers, 'x-archive-meta-' . $h, $v->[0]);
	    } else {
		foreach my $i (0..$#{$v}) {
		    push(@headers,
			 sprintf('x-archive-meta%02d-%s', $i + 1, $h),
			 $v->[$i]);
		}
	    }
	} else {
	    push(@headers, 'x-archive-meta-' . $h, $v);
	}
    }

    unless ($keepExistingMetadata) {
	push(@headers, 'x-archive-ignore-preexisting-bucket', '1');
    }
    
    if ($verbose) {
	print STDERR "PUT $uri\n";
	for (my $i = 0; $i < $#headers; $i += 2) {
	    print STDERR $headers[$i], ":", $headers[$i + 1], "\n";
	}
    }
    if ($dryrun) {
	print STDERR "## dry-run; not making actual request\n";
    } else {
	# use of custom PUT_FILE is for efficient handling of large files.
	# see comment on PUT_FILE above.
	my $req = PUT_FILE $uri, $content, @headers;
	#print STDERR $req->as_string;
	my $res = $ua->request($req);
	print STDERR "\n";
	if ($res->is_success) {
	    print $res->status_line, "\n", $res->content, "\n";
	} else {
	    print $res->status_line, "\n", $res->content, "\n";
	    # TODO: record failures and prepare for retry
	}
    }
    $curCollection = $collection;
    $curItem = $item;
}
close(MT);
# TODO show some statistics
