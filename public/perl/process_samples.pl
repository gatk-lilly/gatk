#!/usr/bin/perl -w

use strict;
use Data::Dumper;
use File::Path;

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

sub create_condor_submission_file {
	my ($id, @args) = @_;

	my $jobdir = ".condor_submit/$id";

	my $submission_file = "$jobdir/submit";
	if (! -e $jobdir) {
		mkpath($jobdir);
	}

	open(CSF, ">$submission_file");

	print CSF "Universe = vanilla\n";
	#print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID == 1)\n";
	print CSF "Requirements = (OpSys =?= \"LINUX\")\n";
	print CSF "Executable = $ENV{'HOME'}/opt/GATK-Lilly/public/shell/process_one_sample.sh\n";
	print CSF "Arguments = " . join(" ", @args) . "\n";
	print CSF "input = /dev/null\n";
	print CSF "output = $jobdir/log.out\n";
	print CSF "error = $jobdir/log.err\n";
	print CSF "notification = Never\n";
	print CSF "should_transfer_files = YES\n";
	print CSF "when_to_transfer_output = ON_EXIT\n";
	print CSF "Queue\n";

	close(CSF);

	return $submission_file;
}

sub get_s3_contents {
	my ($s3_path) = @_;
	my %s3;

	chomp(my @files = qx(s3cmd ls -r $s3_path));

	foreach my $line (@files) {
		my ($date, $time, $size, $file) = split(/\s+/, $line);

		$s3{$file} = $size;
	}

	return %s3;
}

my %args = &getArgs("s3_manifest" => undef, "s3_lane_path" => undef, "s3_upload_path" => undef, "run" => 0);

my ($s3_root) = $args{'s3_upload_path'} =~ /(s3:\/\/.+?\/)/;
my %s3 = &get_s3_contents($s3_root);

my @entries;

open(MANIFEST, $args{'s3_manifest'});
chomp(my $header = <MANIFEST>);
my @header = split(/\s+/, $header);

while (my $line = <MANIFEST>) {
	next if $line =~ /#/;
	chomp($line);

	my @entry = split(/\s+/, $line);
	my %entry;

	for (my $i = 0; $i <= $#entry; $i++) {
		$entry{$header[$i]} = $entry[$i];
	}

	push(@entries, \%entry);
}
close(MANIFEST);

my %samples;
foreach my $entry (@entries) {
	my %entry = %$entry;
	my $lanebam = "$args{'s3_lane_path'}/$entry{'sample'}/$entry{'flowcell'}.$entry{'lane'}/$entry{'flowcell'}.$entry{'lane'}.bam";
	push(@{$samples{$entry{'sample'}}}, $lanebam);
}

foreach my $sample (keys(%samples)) {
	my @s3lanebams = @{$samples{$sample}};
	my $s3samplebam = "$args{'s3_upload_path'}/$sample/$sample.bam";

	if (exists($s3{$s3samplebam})) {
		print "Skipping sample-level pipeline for $sample because the result is already present in S3.\n";
	} else {
		my $allLanesPresent = 1;
		foreach my $s3lanebam (@s3lanebams) {
			if (!exists($s3{$s3lanebam})) {
				$allLanesPresent = 0;
			}
		}

		if ($allLanesPresent) {
			foreach my $chr ("chrM", "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY") {
				my @cmdargs = ($sample, $chr, $s3_root, $args{'s3_upload_path'}, @s3lanebams);

				#my $cmd = "$ENV{'HOME'}/opt/GATK-Lilly/public/shell/process_one_sample.sh " . join(" ", @cmdargs);
				#print "$cmd\n";

				my $submission_file = &create_condor_submission_file($sample, @cmdargs);
				my $submission_cmd = "condor_submit $submission_file";

				if ($args{'run'} == 1) {
					print "Dispatching sample-level pipeline for $sample ($submission_file)\n";
					system($submission_cmd);
				} else {
					print "Simulating dispatch of sample-level pipeline for $sample ($submission_file)\n";
					print "$submission_cmd\n";
				}
			}
		} else {
			print "Skipping sample-level pipeline for $sample because some expected lane BAMs are not present in S3.\n";
		}
	}
}
