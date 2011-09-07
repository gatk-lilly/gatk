#!/usr/bin/perl -w

# Johny
# All commented lines are written by Kiran to make it run on Cloud env

# Before you run this script 
# Before run condor or this script ( This script is calling the command condor_submit
# At prompt type "bash" enter  
# Source ~/.bashrc
# perl process_lanes_local.pl --manifest_file=jki_manifestfile_secondlane.txt --output_upload_path=/lrlhps/scratch/u9x8503/ngs/GATK/lanes --run=1 &


use strict;
use Data::Dumper;
use File::Path;

my $app_path = "/lrlhps/users/c085541/GATK"; # Change according to your application path which starts with \opt\

if ($app_path =~ /\/$/){
	$app_path =~ s/\/$//;
}
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
	print "Example =>  perl process_lanes_local.pl --manifest_file=jki_manifestfile_secondlane.txt --output_upload_path=/lrlhps/scratch/u9x8503/ngs/GATK/lanes --run=1 & \n\n";

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

	#my $jobdir = ".condor_submit/$id";
	my $jobdir = ".condor_submit";
        my $logprefix = $id;
        my $submission_file = "$jobdir/$logprefix".".submit";

	#my $submission_file = "$jobdir/submit";
	if (! -e $jobdir) {
		mkpath($jobdir);
	}

	open(CSF, ">$submission_file");

	print CSF "Universe = vanilla\n";
	#print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID == 1)\n";
	#print CSF "Requirements = (OpSys =?= \"LINUX\")\n";
	#print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID == 1) && (environmentName == \"toolbox\")\n"; #To make it run on any toolbox servers

	print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID == 1) && (Machine == \"ratchet.am.lilly.com\")\n";

	print CSF "Executable = $app_path/opt/GATK-Lilly/public/shell/process_one_paired_end_lane_local.sh\n";
	print CSF "Arguments = " . join(" ", @args) . "\n";
	print CSF "input = /dev/null\n";
	print CSF "output = $jobdir/$logprefix".".out\n";
	print CSF "error = $jobdir/$logprefix".".err\n";
	print CSF "StreamOut = True\n";
	print CSF "StreamErr = True\n";	
	print CSF "notification = Error\n";
	print CSF "should_transfer_files = YES\n";
	print CSF "when_to_transfer_output = ON_EXIT_OR_EVICT\n";
	print CSF "Queue\n";

	close(CSF);
	
#	my $local_run_command = $ENV{'HOME'}."/opt/GATK-Lilly/public/shell/process_one_paired_end_lane_local.sh  ". join(" ", @args) ;
	my $local_run_command = "$app_path/opt/GATK-Lilly/public/shell/process_one_paired_end_lane_local.sh  ". join(" ", @args) ;

	return ($submission_file,  $local_run_command );
}

sub get_local_contents {   # this sub will collect all the processed or processing .bam files from lanes folder (output folder) to skip if processed.
	my ($s3_path) = @_;
	my %s3;

#	chomp(my @files = qx(s3cmd ls -r $s3_path));
	chomp(my @files = qx(ls -1R $s3_path));
	my $current_path="";
	foreach my $line (@files) {
		chomp $line;
				
		if ($line =~ /\.bam/){
			my $file = $current_path."/".$line;  # appending file path :Johny
			$s3{$file} = "";
		}elsif($line =~ /($s3_path)/ ){
			$line =~ s/\://g;
			$current_path = $line;
		}
	}	
	return %s3;
}

# sub get_s3_contents {
# 	my ($s3_path) = @_;
# 	my %s3;

# 	chomp(my @files = qx(s3cmd ls -r $s3_path));

# 	foreach my $line (@files) {
# 		my ($date, $time, $size, $file) = split(/\s+/, $line);

# 		$s3{$file} = $size;
# 	}

# 	return %s3;
# }

my %args = &getArgs("manifest_file" => undef, "output_upload_path" => undef, "run" => 0);

#my ($s3_root) = $args{'output_upload_path'} =~ /(s3:\/\/.+?\/)/;
my ($s3_root) = $args{'output_upload_path'};
my %s3 = &get_local_contents($s3_root);

my @entries;

open(MANIFEST, $args{'manifest_file'});
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

	my $s3file = "$args{'output_upload_path'}/$entry{'sample'}/$entry{'flowcell'}.$entry{'lane'}/$entry{'flowcell'}.$entry{'lane'}.bam";

	if ($entry{'f1'} eq 'none' || $entry{'f2'} eq 'none') {
		print "Skipping lane-level pipeline for $entry{'sample'} $rgid because fastqs for one or both ends of the lane are missing.\n";
	} elsif (exists($s3{$s3file})) {
		print "Skipping lane-level pipeline for $entry{'sample'} $rgid because the result is already present in S3.\n";
	} else {
		my @cmdargs = ($rgid, $rgsm, $rglb, $rgpu, $rgpl, $rgcn, $rgdt, $entry{'flowcell'}, $entry{'lane'}, $entry{'f1'}, $entry{'f2'}, $s3_root, $args{'output_upload_path'});

# 		my $cmd = "$ENV{'HOME'}/opt/GATK-Lilly/public/shell/process_one_paired_end_lane.sh " . join(" ", @cmdargs);
# 		print "$cmd\n";

		my ($submission_file, $local_run_command) = &create_condor_submission_file("$rgsm.$rgid", @cmdargs);
		my $submission_cmd = "condor_submit $submission_file";

		if ($args{'run'} == 1) {
			print "Dispatching lane-level pipeline for $entry{'sample'} $rgid ($submission_file)\n";
			system($submission_cmd);    # Process will be taken care by condor
			#system($local_run_command); # Process will be running locally without contor support
			
		} else {
			print "Simulating dispatch of lane-level pipeline for $entry{'sample'} $rgid ($submission_file)\n";
			print "$submission_cmd\n";
			#print "$local_run_command\n";
		}
	}
}
# for reference - 
# export ID=$1
# export SM=$2
# export LB=$3
# export PU=$4
# export PL=$5
# export CN=$6
# export DT=$7
# export FLOWCELL=$8
# export LANE=$9
# export FQ1=${10}
# export FQ2=${11}
# export S3_BUCKET=`echo ${12} | sed 's/s3:\/\///' | sed 's/\/$//'`
# export output_upload_path=`echo ${13}/$SM/$FLOWCELL.$LANE | sed 's/s3:\/\///'`
