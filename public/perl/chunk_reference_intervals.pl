#!/usr/local/bin/perl -w

use strict;

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

my %args = &getArgs("reference_dict" => undef, "chunk_size" => 1000000);

open(DICT, $args{'reference_dict'});
chomp(my @dict = <DICT>);
close(DICT);

my @contigs;

foreach my $dictline (@dict) {
	if ($dictline =~ /SN:(.+?)\s/) {
		my $contig = $1;
		my ($length) = $dictline =~ /LN:(\d+?)\s/;

		push(@contigs, { 'contig' => $contig, 'length' => $length });
	}
}

print join("\n", @dict) . "\n";

foreach my $entry (@contigs) {
	my %entry = %$entry;

	my $numchunks = int($entry{'length'}/$args{'chunk_size'});

	if ($numchunks < 1.0) {
		print "$entry{'contig'}\t1\t$entry{'length'}\t+\t$entry{'contig'}.target_1\n";
	} else {
		for (my $chunk = 0; $chunk < $numchunks; $chunk++) {
			my $start = ($chunk*$args{'chunk_size'}) + 1;
			my $end = ($chunk == $numchunks - 1) ? $entry{'length'} : ($chunk + 1)*$args{'chunk_size'};

			print "$entry{'contig'}\t$start\t$end\t+\t$entry{'contig'}.target_$chunk\n";
		}
	}
}
