#!/usr/bin/perl -w

use strict;

my @hostlines = qx(qhost | grep amd);

foreach my $hostline (@hostlines) {
	my ($host, $junk) = split(/\s+/, $hostline);

	my $cmd = "ssh $host " . join(" ", @ARGV);
	print "$cmd\n";

	#chomp(my @results = qx($cmd));
	#print "\t" . join("\n\t", @results) . "\n\n";

	eval {
		local $SIG{ALRM} = sub { die "alarm\n" }; # NB: \n required
		alarm 10;
		#$nread = sysread SOCKET, $buffer, $size;

		chomp(my @results = qx($cmd));
		print "\t" . join("\n\t", @results) . "\n\n";

		alarm 0;
	};
	if ($@) {
		die unless $@ eq "alarm\n";   # propagate unexpected errors
		# timed out

		print "\t**timed out**\n\n";
	} else {
	  # didn't
	}
}

