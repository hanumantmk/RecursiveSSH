#!/usr/bin/perl -w

use strict;

use RecursiveSSH;

use Getopt::Long;

my @users;
my @logons;
GetOptions(
  "user=s"  => \@users,
  "logon=s" => \@logons,
  'help|?'  => sub {HELP(0)},
);

$SIG{INT} = \&RecursiveSSH::clean_up;

my (@machines) = @ARGV;

@machines or print "Please enter some machines\n" and HELP(1);

my $person = @users ? join('|', @users) : '.';

my $rssh = RecursiveSSH->new({
  data => {
    i        => 0,
    machines => \@machines,
    person   => $person,
    logons   => [map { [split /=/] } @logons],
  },
  children => sub {
    my $self = shift;

    my $data = $self->data;
    my @rval;

    my $person = $data->{person};

    if ($data->{i} == 0) {
      @rval = @{$data->{machines}};
    } else {
      @rval = keys %{{map {
	chomp;
	my @line = split /\s+/;

        my $i;
	for ($i = $#line; $i >= 0; $i--) {
	  last unless ($line[$i] =~ /-/);
	}

	if ($i) {
	  my $machine = $line[$i];

	  $machine = [split /@/, $machine]->[1] if $machine =~ /@/;

	  if ($machine =~ /['"]/) {
	    ()
	  } else {
	    ($machine, 1);
	  }
	} else {
	  ()
	}
      } `ps -C ssh -o user,command | tail -n +2 | egrep '$person'`}};
    }

    @rval = map {
      my $machine = $_;

      foreach (@{$data->{logons}}) {
	my ($regex, $user) = @$_;

	if ($machine =~ /$regex/) {
	  $machine = join('@', $user, $machine);
	}
      }

      $machine;
    } @rval;

    $data->{i}++;

    return @rval;
  },
  failed_host_cb => sub { print "FAILED HOST: " . join('->', @{$_[0]}) . "\n" },
});

$rssh->connect;

$rssh->exec(
  sub { $_[0]->hostname },
  [],
  sub { print "HOST: " . join('->', @{$_[0]}) . "\n" },
  sub { print "\n\nHosts all finished...\n\n" },
);

$rssh->loop;

$rssh->exec(
  sub {
    my $self = shift;

    my $data = $self->data;

    my $person = $data->{person};

    if (my @lines = `who | egrep '$person' | sort`) {
      return(join('',
	join("->", @{$self->hostname}) . "\n",
	map { "\t$_" } @lines,
      ));
    }

    return;
  },
  [],
  sub {
    my $output = shift;
    print "WHO: " . $output . "\n" if defined $output;
  },
);

$rssh->exec(
  sub {
    my $self = shift;

    my $data = $self->data;

    my $person = $data->{person};

    if (my @lines = `ps -C ssh -o user,command | tail -n +2 | egrep '$person' | grep -v sysread`) {
      return(join('',
	join("->", @{$self->hostname}) . "\n",
	map { "\t$_" } @lines,
      ));
    }

    return;
  },
  [],
  sub {
    my $output = shift;
    print "CONNECTIONS: " . $output . "\n" if defined $output;
  },
);

$rssh->loop;

exit 0;

sub HELP {
  my $exit = shift;

  print <<HELP
USAGE: $0 [options] machine1 [machine2 ...]

OPTIONS:
--user  users to search for, multiple may be specified.  Without a user,
        searches for all non-root users.

--logon logon to use for a machine.  I.e. --logon machine_regex=user_a

--help  this help message
HELP
;

  exit $exit;
}
