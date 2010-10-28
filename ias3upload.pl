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

use constant IAS3URLBASE => 'http://s3.us.archive.org/';
use constant ENV_AUTHKEYS => 'IAS3KEYS';

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

# obsoleted code reading file content into string of bytes.
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
# efficiently. HTTP::Request::Common->POST has such feature built-in
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
}

my $ias3keys = $ENV{ENV_AUTHKEYS()};
unless ($ias3keys) {
    warn "## You could set your ACCESSKEY:SECRETKEY to ", ENV_AUTHKEYS, " environment variable\n";
}
# controls
my $dryrun = 0;
my $verbose = 0;
my $metatbl = 'metadata.csv';	# CSV file having metadata for each item

# default metadata from command line
my %metadefaults;

my $ignorePreexistingBucket = 0;
my $keepOldVersion = 0;
my $cascadeDelete = 0;
my $noDerive = 0;

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
	   # control options
	   'ignore-preexisting-bucket'=>\$ignorePreexistingBucket,
	   # not yet supported control options
	   'keep-old'=>\$keepOldVersion,
	   'cascade-delete'=>\$cascadeDelete,
	   'no-derive'=>\$noDerive,
    );

unless (defined $ias3keys) {
    die "ERROR:".
	"specify IAS3 key pair with -k (or ".ENV_AUTHKEYS." environment variable). ",
	"visit http://www.archive.org/account/s3.php if you don't have it yet.\n";
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
	die "ERROR:bad metadata name ", escapeText($fieldnames[$i]), " in column $i\n";
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
foreach my $cn (('item', 'description', 'creator', 'mediatype', 'collection')) {
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

    if ($ignorePreexistingBucket) {
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
