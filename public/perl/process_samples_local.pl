#!/usr/bin/perl -w

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
	print "Example =>  perl process_samples_local.pl --manifest_file=jki_manifestfile_secondlane.txt --lane_path=/lrlhps/scratch/u9x8503/ngs/GATK/lanes --output_upload_path=/lrlhps/scratch/u9x8503/ngs/GATK/samples --run=1 \n\n";

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
	my ($id, $chr, @args) = @_;
	my $jobdir = ".condor_submit";
	my $logprefix = $id."_".$chr."_";
	my $submission_file = "$jobdir/$logprefix"."submit";

	my $log_file = "$jobdir/$logprefix"."log.out";
	my $has_run_before = 0;

	if (-e $submission_file) {
		$has_run_before = 1;
	}

	if (! -e $jobdir) {
		mkpath($jobdir);
	}

	open(CSF, ">$submission_file");

	print CSF "Universe = vanilla\n";
	#print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID == 1)\n";
	#print CSF "Requirements = (OpSys =?= \"LINUX\")\n";	
	print CSF "Requirements = (OpSys =?= \"LINUX\") && (SlotID ==1) && (environmentName == \"toolbox\")\n"; #To make it run on any toolbox servers
#	print CSF "Executable = $ENV{'HOME'}/opt/GATK-Lilly/public/shell/process_one_sample_local.sh\n";
	print CSF "Executable = $app_path/opt/GATK-Lilly/public/shell/process_one_sample_local.sh\n";
	print CSF "Arguments = " . join(" ", @args) . "\n";
	print CSF "input = /dev/null\n";
	print CSF "output = $jobdir/$logprefix"."log.out\n";
	print CSF "error = $jobdir/$logprefix"."log.err\n";
	print CSF "StreamOut = True\n";
	print CSF "StreamErr = True\n";
	print CSF "notification = Error\n";
	print CSF "notify_user = jian.wang\@lilly.com\n";
	print CSF "should_transfer_files = YES\n";
	print CSF "when_to_transfer_output = ON_EXIT_OR_EVICT\n";
	print CSF "Queue\n";

	close(CSF);
	
	# Johny added to get the command 
	my $local_run_command = "$app_path/opt/GATK-Lilly/public/shell/process_one_sample_local.sh " . join(" ", @args);
	
	return($submission_file, $log_file, $has_run_before, $local_run_command);
}

sub get_local_contents {   # this sub will collect all the processed or processing .bam files from resource folder 
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

my %args = &getArgs("manifest_file" => undef, "lane_path" => undef, "output_upload_path" => undef, "run" => 0);

#my ($s3_root) = $args{'output_upload_path'} =~ /(s3:\/\/.+?\/)/;

# get all lane bam files 
my ($s3_root) = $args{'lane_path'};
my %lanes_available = &get_local_contents($s3_root);

# get all sample bam files 
$s3_root = $args{'output_upload_path'};
my %samples_available = &get_local_contents($s3_root);

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
# manifest file lanes are below
my %samples;
foreach my $entry (@entries) {
	my %entry = %$entry;
	my $lanebam = "$args{'lane_path'}/$entry{'sample'}/$entry{'flowcell'}.$entry{'lane'}/$entry{'flowcell'}.$entry{'lane'}.bam";
	push(@{$samples{$entry{'sample'}}}, $lanebam);
}

foreach my $chr ("chr22", "chr21", "chr20", "chr19", "chr18", "chr17", "chr16", "chr15", "chr14", "chr13", "chr12", "chr11", "chr10", "chr9", "chr8", "chr7", "chr6", "chr5", "chr4", "chr3", "chr2", "chr1", "chrX", "chrY") {
	foreach my $sample (keys(%samples)) {
		my @s3lanebams = @{$samples{$sample}};

		my $allLanesPresent = 1;
		foreach my $s3lanebam (@s3lanebams) {
			if (!exists($lanes_available{$s3lanebam})) {
				$allLanesPresent = 0;
			}
		}

		#if (!$allLanesPresent) {
		#	print "Skipping sample-level pipeline for $sample because some expected lane BAMs are not present in S3.\n";
		#} else {
			my $s3samplebam = "$args{'output_upload_path'}/$sample/$sample.$chr.analysis_ready.bam";

			if ($samples_available{$s3samplebam}) {
				print "Skipping sample-level pipeline for $sample $chr because the result is already present in S3.\n";
			} else {
				my @cmdargs = ($sample, $chr, $s3_root, $args{'output_upload_path'}, @s3lanebams);
				print join ("*",@s3lanebams);
				my ($submission_file, $log_file, $has_run_before, $local_run_command) = &create_condor_submission_file($sample, $chr, @cmdargs);
#print "\n\n ($submission_file, $log_file, $has_run_before, $local_run_command)\n\n";
				#print "Log file: $log_file\n";
				#print "Size: " . (-s $log_file) . "\n";

				# has run before        log file exists           log size is 0        meaning
				# 1                     0                         0                    dispatched but never started
				# 1                     1                         0                    dispatched still running
				# 1                     0                         1                    can't happen
				# 1                     1                         1                    job dispatched and finished
				# 0                     0                         0                    never run
				# 0                     1                         0                    can't happen
				# 0                     0                         1                    can't happen

				if ($has_run_before == 1 && -e $log_file && -s $log_file == 0) {
					print "Skipping sample-level pipeline for $sample $chr because the job is already running.\n";
				} else {
					my $submission_cmd = "condor_submit $submission_file";

					if ($args{'run'} == 0) {
						print "Simulating dispatch of sample-level pipeline for $sample $chr ($submission_file)\n";
						print "$submission_cmd\n";
						#print "$local_run_command\n";
					} else {
						print "Dispatching sample-level pipeline for $sample $chr ($submission_file)\n";
						system($submission_cmd);
						#system($local_run_command);
					}
				}
			}
		#}
	}
}
