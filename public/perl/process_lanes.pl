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

  	my $jobdir = ".condor_submit";
	my $logprefix = $id."_";
	my $submission_file = "$jobdir/$logprefix"."submit";
	my $log_file = "$jobdir/$logprefix"."log.out";

	if (! -e $jobdir) {
		mkpath($jobdir);
	}

	open(CSF, ">$submission_file");

	print CSF "Universe = vanilla\n";
	print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID == 1)\n";
	print CSF "Executable = $ENV{'HOME'}/opt/GATK-Lilly/public/shell/process_one_paired_end_lane.sh\n";
	print CSF "Arguments = " . join(" ", @args) . "\n";
	print CSF "input = /dev/null\n";
	print CSF "output = $jobdir/$logprefix"."log.out\n";
	print CSF "error = $jobdir/$logprefix"."log.err\n";
	print CSF "StreamOut = True\n";
	print CSF "StreamErr = True\n";
	print CSF "notification = Error\n";
	print CSF "should_transfer_files = YES\n";
	print CSF "when_to_transfer_output = ON_EXIT_OR_EVICT\n";
	print CSF "Queue\n";

	close(CSF);

	return ($submission_file, $log_file, $has_run_before);
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

my %args = &getArgs("s3_manifest" => undef, "s3_upload_path" => undef, "run" => 0);

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

foreach my $entry (@entries) {
	my %entry = %$entry;

	my $bamName = "$entry{'flowcell'}.$entry{'lane'}";
	(my $shortFlowcell = $entry{'flowcell'}) =~ s/A.XX//;

	my ($year, $month, $day) = split(/-/, $entry{'date'});
	$year =~ s/^\d\d//;
	my $shortDate = "$year$month$day";

	my $rgid = "$shortFlowcell.$entry{'lane'}";
	my $rgsm = $entry{'sample'};
	my $rglb = $entry{'library'};
	my $rgpu = "$entry{'flowcell'}$shortDate.$entry{'lane'}";
	my $rgpl = "ILLUMINA";
	my $rgcn = "BGI";
	my $rgdt = $entry{'date'};

	my $s3file = "$args{'s3_upload_path'}/$entry{'sample'}/$entry{'flowcell'}.$entry{'lane'}/$entry{'flowcell'}.$entry{'lane'}.bam";

	if ($entry{'f1'} eq 'none' || $entry{'f2'} eq 'none') {
		print "Skipping lane-level pipeline for $entry{'sample'} $rgid because fastqs for one or both ends of the lane are missing.\n";
	} elsif (exists($s3{$s3file})) {
		print "Skipping lane-level pipeline for $entry{'sample'} $rgid because the result is already present in S3.\n";
	} else {
		my @cmdargs = ($rgid, $rgsm, $rglb, $rgpu, $rgpl, $rgcn, $rgdt, $entry{'flowcell'}, $entry{'lane'}, $entry{'f1'}, $entry{'f2'}, $s3_root, $args{'s3_upload_path'});

		#my $cmd = "$ENV{'HOME'}/opt/GATK-Lilly/public/shell/process_one_paired_end_lane.sh " . join(" ", @cmdargs);
		#print "$cmd\n";

		my $submission_file = &create_condor_submission_file("$rgsm.$rgid", @cmdargs);
		my $submission_cmd = "condor_submit $submission_file";

		if ($args{'run'} == 1) {
			print "Dispatching lane-level pipeline for $entry{'sample'} $rgid ($submission_file)\n";
			system($submission_cmd);
		} else {
			print "Simulating dispatch of lane-level pipeline for $entry{'sample'} $rgid ($submission_file)\n";
			print "$submission_cmd\n";
		}
	}
}
