#!/usr/bin/perl -w

use strict;

use RecursiveSSH;

use Getopt::Long;

my @users;
GetOptions(
  "user=s" => \@users,
  'help|?' => sub {HELP(0)},
);

my (@machines) = @ARGV;

@machines or print "Please enter some machines\n" and HELP(1);

my $person = @users ? join('|', @users) : '.';

RecursiveSSH::bootstrap(
  data => {
    i        => 0,
    machines => \@machines,
    person   => $person,
  },
  children => sub {
    my $data = shift;
    my @rval;

    my $person = $data->{person};

    if ($data->{i} == 0) {
      @rval = @{$data->{machines}};
    } else {
      @rval = keys %{{map {
	my @line = split /\s+/;

        my $i;
	for ($i = $#line; $i >= 0; $i--) {
	  last unless ($line[$i] =~ /-/);
	}

	$i ? ($line[$i], 1) : ()
      } grep {
	! /@/
      } `ps -C ssh -o user,command | tail -n +2 | grep -v root | grep -v -- -l | egrep '$person'`}};
    }

    $data->{i}++;

    return @rval;
  },
  run => sub {
    my $data = shift;
    my $person = $data->{person};

    if (my @lines = `who | egrep '$person' | grep -v root | grep -v -- -l | sort`) {
      return join('',
	join("->", @$RecursiveSSH::hostname) . "\n",
	map { "\t$_" } @lines,
      );
    } else {
      return;
    }
  },
);

sub HELP {
  my $exit = shift;

  print <<HELP
USAGE: $0 [options] machine1 [machine2 ...]

OPTIONS:
--user  users to search for, multiple may be specified.  Without a user,
        searches for all non-root users.

--help  this help message
HELP
;

  exit $exit;
}
