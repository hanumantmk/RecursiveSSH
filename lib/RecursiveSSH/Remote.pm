package RecursiveSSH::Remote;

use strict;
use warnings;

our $VERSION = 0.001;

=pod

=head1 NAME

RecursiveSSH::Remote

=head1 SYNOPSIS

my $remote = RecursiveSSH::Remote->new({
  header        => $stringification # of RecursiveSSH::Remote,
  find_children => $children_sub,
  data          => $local_storage_for_children,
  hostname      => $arrayref_of_hops,
});

$remote->recurse;

=head1 DESCRIPTION

An object capable of bootstrapping itself onto other machines via piping into a
perl eval.  Holds a find_children function which is used to find additional
children.  Exists as a parent child fork pair which allows the parent to
collect writes from above and transmit to the child when the child is ready (or
signal in the case of control packets).

After the initial sprawl out based on $find_children, parent and child settle
into their own select loops which involve forwarding packets and acting on
execs.

=head1 METHODS

=over 4

=cut

use Data::Dumper;
use IPC::Open3;
use IO::Select;
use List::Util qw( first );

use base 'Exporter';

our @EXPORT_OK = qw(
  read_packet
  put_packet
  slave_invocation
  put
);

our $EVAL;

=item $class->new({})

Arguments are: {
  header        => $stringification # of RecursiveSSH::Remote,
  find_children => $children_sub,
  data          => $local_storage_for_children,
  hostname      => $arrayref_of_hops,
}

Provides a new object on the remote side to communicate with and access in
callbacks.

=cut

sub new {
  my ($class, $options) = @_;

  $options ||= {};

  my $self = bless $options, $class;

  return $self;
}

=item $self->data()

Getter for the data member

=cut

sub data { $_[0]->{data} }

=item $self->hostname()

Getter for the hostname member

=cut

sub hostname { $_[0]->{hostname} }

sub _build_children {
  my $self = shift;

  my %hosts = map { $_, 1 } @{$self->{hostname}};

  my %children = eval {
    map { $_, {} } grep { ! $hosts{$_} } $self->{find_children}->($self);
  };
  debug("Error in find_children: $@") if $@;

  foreach my $machine (keys %children) {
    my $data = $children{$machine};

    my $program = $self->program($machine);

    my $length = length($program);

    my $cmd = ssh_invocation($machine, $length);

    $data->{pid} = open3($data->{w}, $data->{r}, $data->{r}, $cmd);

    put($data->{w}, $program);
  }

  $self->{children} = \%children;

  return;
}

sub _recurse_parent {
  my ($self, $child_fh, $kill_sub) = @_;

  $0 = 'RecursiveSSH Parent';

  my @queue;

  my $jump_for_me = {
    'quit' => $kill_sub,
    'exec' => sub {
      my $packet = shift;

      push @queue, $packet;
    },
    'error' => sub {
      debug("Error packet, dunno");
      $kill_sub->();
    },
    'debug' => sub {
      debug("We shouldn't be passing debug packets up");
      $kill_sub->();
    }
  };

  my $read  = IO::Select->new(\*STDIN);
  my $write = IO::Select->new($child_fh);

  while (my ($reading, $writing) = IO::Select->select($read, $write, undef)) {
    if (@$reading) {
      my $packet = read_packet(\*STDIN);
      if (my $sub = $jump_for_me->{$packet->{type}}) {
	$sub->($packet);
      } else {
	debug("garbage on STDIN");
	$kill_sub->();
      }
    } elsif (@$writing && @queue) {
      put_packet($child_fh, shift @queue);
    } else {
      $read->can_read;
    }
  }

  $kill_sub->();
}

sub route_info_for_packet {
  my ($self, $packet) = @_;

  my $src = $packet->{src};

  my $dest = $packet->{dest};

  if (ref $dest eq 'ARRAY') {
    my %route;

    my $me = $self->hostname;

    my $me_len = scalar(@$me);

    HOST: foreach my $host (@$dest) {
      my $host_len = scalar(@$host);

      if ($host_len < $me_len) {
	push @{$route{''}}, $host;
	next;
      }

      for (my $i = 0; $i < $me_len; $i++) {
	if ($host->[$i] ne $me->[$i]) {
	  push @{$route{''}}, $host;
	  next HOST;
	}
      }

      if ($host_len > $me_len) {
	push @{$route{$host->[$me_len]}}, $host;
      }
    }

    return \%route;
  } elsif ($dest eq 'broadcast') {
    return 'broadcast';
  } else {
    debug("invalid dest: $dest for packet: " . Dumper($packet));
    return;
  }
}

sub needs_routing {
  my ($self, $packet) = @_;

  if (grep { $packet->{type} eq $_ } qw( error debug failed_host )) {
    return 1;
  } else {
    my $route = $self->route_info_for_packet($packet);
    return 1 if ($route eq 'broadcast' || (ref $route eq 'HASH' && %$route));
  }

  return 0;
}

sub for_me {
  my ($self, $packet) = @_;

  return 0 if (grep { $packet->{type} eq $_ } qw( error debug failed_host ));

  my $me = $self->hostname;

  my $me_len = scalar(@$me);

  my $dest = $packet->{dest};

  if (ref $dest eq 'ARRAY') {
    HOST: foreach my $host (@$dest) {
      my $host_len = scalar(@$host);

      next if $host_len != $me_len;

      for (my $i = 0; $i < $me_len; $i++) {
	next HOST if ($host->[$i] ne $me->[$i]);
      }

      return 1;
    }

    return;
  } elsif ($dest eq 'broadcast') {
    return 1;
  } else {
    debug("invalid dest: $dest for packet: " . Dumper($packet));
    return;
  }
}

sub _recurse_child {
  my $self = shift;

  $0 = 'RecursiveSSH Child';

  $self->_build_children;

  my $children = $self->{children};

  my $end_sub = $SIG{HUP} = sub {
    put_packet($_->{w}, {type => 'quit'}) for values %$children;

    waitpid($_->{pid}, 0) for values %$children;

    exit 0;
  };

  my %execs;

  my %world = (%$children, '', { r => \*STDIN, w => \*STDOUT });

  my $add_exec_entry = sub {
    my ($packet, $machine) = @_;

    my @hostname = @{$self->hostname};

    if ($machine eq '') {
      pop @hostname;
    } else {
      push @hostname, $machine;
    }

    $execs{$packet->{id}} = {
      orig    => $machine,
      dest    => [\@hostname],
      running => {},
    };
  };

  my %jump_for_me = (
    done => sub {
      my ($packet, $machine) = @_;

      my $id = $packet->{id};

      if (exists $execs{$id}) {
	delete $execs{$id}{running}{$machine};
      } else {
	debug("Something is wrong with $id on " . Dumper($self->hostname, \%execs));
	return;
      }
    },
    exec => sub {
      my ($packet, $machine) = @_;

      eval {
	my $r = $packet->{data}->($self);

	put_packet($world{$machine}{w}, {type => 'result', data => $r, id => $packet->{id}, dest => [$packet->{src}], src => $self->hostname}) if defined $r;
      };

      debug($@) if $@;

      $add_exec_entry->($packet, $machine) unless $execs{$packet->{id}};
    },
  );

  my %jump_for_dispatch = (
    debug       => sub { my $packet = shift; put_packet(\*STDOUT, $packet) },
    error       => sub { my $packet = shift; put_packet(\*STDOUT, $packet) },
    failed_host => sub { my $packet = shift; put_packet(\*STDOUT, $packet) },
    result => sub {
      my ($packet, $machine) = @_;

      my $route = $self->route_info_for_packet($packet);

      if (ref $route eq 'HASH') {
	foreach my $m (keys %$route) {
	  put_packet($world{$m}{w}, {%$packet, dest => $route->{$m}});
	}
      } elsif ($route eq 'broadcast') {
	debug("Can't broadcast results");
      }
    },
    exec => sub {
      my ($packet, $machine) = @_;

      my $route = $self->route_info_for_packet($packet);

      my @workers;
      if ($route eq 'broadcast') {
	@workers = keys %$children;
      } elsif (ref $route eq 'HASH') {
	@workers = keys %$route;
      }

      $add_exec_entry->($packet, $machine);

      $execs{$packet->{id}}{running} = {map { $_, 1 } @workers};

      foreach my $w (@workers) {
	my $dest = $route eq 'broadcast' ? 'broadcast' : $route->{$w};
	put_packet($world{$w}{w}, {%$packet, dest => $dest});
      }
    },
  );

  my $select = IO::Select->new(map { $_->{r} } values %world);

  while (1) {
    my ($fh) = $select->can_read();

    my $machine = first { $world{$_}{r} == $fh } keys %world;

    my $packet = read_packet($fh);
    if ($packet && $packet->{type} ne 'error') {
      if ($self->needs_routing($packet)) {
	if (my $sub = $jump_for_dispatch{$packet->{type}}) {
	  $sub->($packet, $machine);
	} else {
	  debug("No logic to route packet: " . Dumper($packet));
	}
      }

      if ($self->for_me($packet)) {
	if (my $sub = $jump_for_me{$packet->{type}}) {
	  $sub->($packet, $machine);
	} else {
	  debug("No logic to locally handle: " . Dumper($packet));
	}
      }

    } else {
      $select->remove($fh);

      waitpid($children->{$machine}{pid}, 0);
      delete $world{$machine};
      delete $children->{$machine};
      delete $execs{$_}{running}{$machine} for keys %execs;

      put_packet(\*STDOUT, { type => 'failed_host', data => [@{$self->hostname}, $machine]});
    }

    foreach my $id (keys %execs) {
      if (! %{$execs{$id}{running}}) {
	if (my $w = $world{$execs{$id}{orig}}) {
	  put_packet($w->{w}, {type => 'done', id => $id, dest => $execs{$id}{dest}, src => $self->hostname});
	}
	delete $execs{$id};
      }
    }
  }

  exit 0;
}

=item $self->recurse()

The main method which triggers recursion and initiates the main select loop

=cut

sub recurse {
  my $self = shift;

  $SIG{PIPE} = 'IGNORE';
  $SIG{__WARN__} = sub {
    my $line = shift;

    debug("WARNING: $line");
  };

  my $child_fh;

  my $pid = open $child_fh, "|-";
  if (! defined $pid) {
    debug("Couldn't fork: $!");
  }

  if ($pid) {
    my $kill_sub = sub {
      kill 1, $pid;
      exit 0;
    };

    eval { $self->_recurse_parent($child_fh, $kill_sub) };
    if ($@) {
      debug("parent died: $@");
      $kill_sub->();
    }
    exit 0;
  } else {
    eval { $self->_recurse_child() };
    if ($@) {
      debug("child died: $@");
      exit 0;
    }
  }
}

=pod

=back

=head1 FUNCTIONS

=over 4

=item read_packet($fh)

reads a packet off the specified file handle

=cut

sub read_packet {
  my $fh = shift;

  my $packed_length = get($fh, 4);

  if (! defined $packed_length) {
    return {
      type => 'error',
      data => "couldn't read header",
      dest => [[]],
    }
  }

  my $length = unpack("N", $packed_length);
  my $data   = get($fh, $length);

  if (! defined $data) {
    return {
      type => 'error',
      data => "couldn't read data",
      dest => [[]],
    }
  }

  my $packet;
  eval $data;
  $@ and debug("read_packet broke: $@\n" . Dumper($data));

  return $packet;
}

=item put_packet($fh, $packet)

Puts a packet onto the specified file handle

=cut

sub put_packet {
  my ($fh, $packet) = @_;

  if (! defined $fh) {
    debug("No defined fh! " . Dumper($packet));
    return;
  }

  my $payload = Data::Dumper->new([$packet],['packet'])->Deparse(1)->Purity(1)->Dump();

  my $length = length($payload);

  put($fh, pack("N", $length) . $payload);
}

sub debug {
  my $string = shift;

  put_packet(\*STDOUT, {type => 'debug', data => $string, dest => [[]]});
}

sub program {
  my ($self, $machine) = @_;

  my $program = join("\n",
    $self->{header},
    Data::Dumper->new([{
      header        => $self->{header},
      find_children => $self->{find_children},
      data          => $self->{data},
      hostname      => [@{$self->{hostname}}, $machine],
    }], ['EVAL'])->Deparse(1)->Purity(1)->Dump(),
    'RecursiveSSH::Remote->new($EVAL)->recurse;',
  ) . "\n";

  return $program;
}

=item put($fh, $string);

puts the specified string onto $fh with syswrite

=cut

sub put {
  my ($fh, $string) = @_;

  my $length = length($string);

  my $wrote = 0;
  do {
    my $w = syswrite($fh, $string, $length - $wrote, $wrote);
    if (! defined $w) {
      return;
    } elsif ($w == 0) {
      return;
    }

    $wrote += $w;
  } while ($wrote < $length);

  return;
}

=item get($fh, $length)

sysreads $length from $fh.

=cut

sub get {
  my ($fh, $length) = @_;

  my $read = 0;
  my $buf = '';

  do {
    my $r = sysread($fh, $buf, $length - $read, $read);

    if (! defined $r) {
#TODO what to do here?
      return;
    } elsif ($r == 0) {
#TODO where do we get this?
      return;
    } else {
      $read += $r;
    }
  } while ($read < $length);

  return $buf;
}

=item slave_invocation($length)

Provides an invocation of perl that can be bootstrapped by writing $length
bytes of program to it.

=cut

sub slave_invocation {
  my $length = shift;
  return 'perl -e \'$l = ' . $length . '; do { $rt = sysread(STDIN, $b, $l - $r, $r); $r += $rt} while ($r < $l); eval $b; $@ and print $@\'';
}

=item ssh_invocation($machine, $length)

Provides an ssh invocation which can be bootstrapped by writing $length bytes to it.

=cut

sub ssh_invocation {
  my ($machine, $length) = @_;

  my $slave = slave_invocation($length);
  $slave =~ s/\$/\\\$/g;

  return 'ssh -oBatchMode=yes -oStrictHostKeyChecking=no -A ' . $machine . ' "' . $slave . '"';

}

1;

=pod

=back

=head1 AUTHOR

Jason Carey

=head1 SEE ALSO

L<RecursiveSSH>

=cut
