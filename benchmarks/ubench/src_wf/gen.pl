#!/opt/local/bin/perl -w

do { print STDERR "Usage: $0 <replicas> [input*]\n"; exit 1; } if @ARGV == 0;

$replicas = shift;

while(<>) {
    while( 1 ) {
	if( m/\!\?(.*)\?\!/ ) {
	    if( $1 eq '=' ) {
		s/\!\?=\?\!/$replicas/;
	    } else {
		my $m = $1;
		my $t = '';
		for( my $i=0; $i < $replicas; ++$i ) {
		    my $mi = $m;
		    $mi =~ s/#/$i/;
		    $t .= $mi;
		}
		s/\!\?(.*)\?\!/$t/;
	    }
	} else {
	    last;
	}
    }
    print;
}
