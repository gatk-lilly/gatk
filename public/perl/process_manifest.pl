#!/usr/bin/perl -w

use strict;
use Data::Dumper;

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

	my $submission_file = ".$id.submit";

	open(CSF, ">$submission_file");

	print CSF "Universe = vanilla\n";
	print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID =?= "1")\n";
	print CSF "Executable = /opt/GATK-Lilly/public/shell/process_one_paired_end_lane.sh\n";
	print CSF "Arguments = " . join(" ", @args) . "\n";
	print CSF "input   = /dev/null\n";
	print CSF "output = .$id-$(Cluster).$(Process).out\n";
	print CSF "error = .$id-$(Cluster).$(Process).err\n";
	print CSF "notification = Never\n";
	print CSF "Queue\n";

	close(CSF);

	return $submission_file;
}

my %args = &getArgs("s3_manifest" => undef, "s3_upload_bucket" => undef, "run" => 0);

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

	#my $cmd = "qsub -l hostname=ip-10-91-2-91.ec2.internal ~/opt/GATK-Lilly/public/shell/process_one_paired_end_lane.sh $rgid $rgsm $rglb $rgpu $rgpl $rgcn $rgdt $entry{'flowcell'} $entry{'lane'} $entry{'f1'} $entry{'f2'}";
	my @cmdargs = ($rgid, $rgsm, $rglb, $rgpu, $rgpl, $rgcn, $rgdt, $entry{'flowcell'}, $entry{'lane'}, $entry{'f1'}, $entry{'f2'});

	print "Dispatching lane-level pipeline for $entry{'sample'} $rgid\n";
	if ($args{'run'} == 1) {
		my $submission_file = &create_condor_submission_file($rgid, @cmdargs);
		system("condor_submit $submission_file");
	}
}
