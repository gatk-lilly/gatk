#!/usr/bin/perl -w

use strict;
use Data::Dumper;
use File::Basename;

sub usage {
	my %args = @_;

	my $maxArgLength = 0;
	foreach my $key (keys(%args)) {
		if (length($key) > $maxArgLength) {
			$maxArgLength = length($key);
		}
	}
	my $format = "   %-${maxArgLength}s     [value=%s]\n";

	print "Usage: $0 [--arg1=value1 --arg2=value2 ...]\n\n";

	print "Required arguments:\n";
	foreach my $arg (sort { $a cmp $b } keys(%args)) {
		if (!defined($args{$arg})) {
			print "   --$arg\n";
		}
	}
	print "\n";

	print "Optional arguments:\n";
	foreach my $arg (sort { $a cmp $b } keys(%args)) {
		if (defined($args{$arg})) {
			printf($format, "--$arg", $args{$arg});
		}
	}
	print "\n";

	exit(0);
}

sub getArgs {
	my %args = @_;

	if (scalar(@ARGV) == 0 || scalar(grep(/-h|-\?|--help/, @ARGV)) > 0) {
		&usage(%args);
	}

	my $args = join(" ", @ARGV);	
	$args =~ s/=\s+/=/g;
	my @args = split(/\s+/, $args);

	foreach my $arg (@args) {
		$arg =~ s/^-+//g;
		my ($key, $value) = split(/=/, $arg);

		if (exists($args{$key})) {
			$args{$key} = $value;
		}
	}

	my $missingValues = 0;
	foreach my $arg (keys(%args)) {
		if (!defined($args{$arg})) {
			print "Argument '$arg' is required, but has not been specified.\n";
			$missingValues = 1;
		}
	}

	if ($missingValues == 1) {
		print "\nSome arguments were missing.  Rerun with the -h argument to get usage information.\n";
		exit(1);
	}

	return %args;
}

my %args = &getArgs("s3_project_path" => undef);

my %records;

my $project = &basename($args{'s3_project_path'});

foreach my $s3dirline (qx(s3cmd ls $args{'s3_project_path'})) {
	$s3dirline =~ s/^\s+//g;
	my ($s3dirtype, $s3dir) = split(/\s+/, $s3dirline);

	foreach my $s3fileline (qx(s3cmd ls $s3dir)) {
		my ($s3filedate, $s3filetime, $s3filesize, $s3file) = split(/\s+/, $s3fileline);

		my $sample = &basename($s3dir);
		(my $fastq = &basename($s3file)) =~ s/\..+//;
		my ($shortDate, $machine, $flowcell, $lane, $library, $end) = split(/_/, $fastq);
		my ($year, $month, $day) = $shortDate =~ /(\d\d)(\d\d)(\d\d)/;
		my $date = "20$year-$month-$day";
		$flowcell =~ s/^FC//;
		$lane =~ s/^L//;

		my $id = "$sample.$flowcell.$lane";

		if (!exists($records{$id})) {
			my %record = ( 'project' => $project, 'sample' => $sample, 'date' => $date, 'machine' => $machine, 'flowcell' => $flowcell, 'lane' => $lane, 'library' => $library, "f$end" => $s3file);

			$records{$id} = \%record;
		} else {
			${$records{$id}}{"f$end"} = $s3file;
		}
	}
}

my @headers = ( 'project', 'sample', 'date', 'machine', 'library', 'flowcell', 'lane', 'f1', 'f2' );
print join("\t", @headers) . "\n";

foreach my $id (sort { $a cmp $b } keys(%records)) {
	if (!defined(${$records{$id}}{'f1'})) {
		${$records{$id}}{'f1'} = "none";
	}

	if (!defined(${$records{$id}}{'f2'})) {
		${$records{$id}}{'f2'} = "none";
	}

	my @entry;

	foreach my $header (@headers) {
		push(@entry, ${$records{$id}}{$header});
	}

	print join("\t", @entry) . "\n";
}
