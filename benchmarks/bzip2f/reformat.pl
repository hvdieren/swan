#!/usr/bin/perl -w

while(<>) {
    chomp;
    s/^([^ ][^ ]+)=//;
    s/ ([^ ][^ ]+)=/ /;
    while( m/ ([0-9]+)m([0-9.]+)s / ) {
	my $t = sprintf "%.03f", ($1*60+$2);
	s/ ${1}m${2}s / $t /;
    }
    s/ ([0-9]+); / $1 /;

    s/ ([1-9])W / $1,improved /;
    s/ ([1-9])w / $1,preserved /;
    s/ ([1-9])P / $1,improved /;
    s/ ([1-9])p / $1,preserved /;
    print "$_\n";
}
