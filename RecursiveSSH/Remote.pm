package RecursiveSSH::Remote;

use strict;
use warnings;

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

sub new {
  my ($class, $options) = @_;

  $options ||= {};

  my $self = bless $options, $class;

  return $self;
}

sub data { $_[0]->{data} }
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

  my $jump_table = {
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
      if (my $sub = $jump_table->{$packet->{type}}) {
	$sub->($packet);
      } else {
	debug("garbage on STDIN");
	$kill_sub->();
      }
    } elsif (@$writing && @queue) {
      put_packet($child_fh, shift @queue);
    }
  }

  $kill_sub->();
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

  my %left;

  my $select = IO::Select->new(\*STDIN, map { $_->{r} } values %$children);

  while (1) {
    my @ready = $select->can_read();

    if ($ready[0] == \*STDIN) {
      shift @ready;

      my $packet = read_packet(\*STDIN);

      if ($packet->{type} ne 'exec') {
	debug("Bad exec packet");
	$end_sub->();
      }

      if (%$children) {
	$left{$packet->{id}} = {map { $_, 1 } keys %$children};

	put_packet($_->{w}, $packet) for values %$children;
      }

      eval {
	my $r = $packet->{data}->($self);

	put_packet(\*STDOUT, {type => 'result', data => $r, id => $packet->{id}});
      };
      debug($@) if $@;

      if (! %$children) {
	put_packet(\*STDOUT, {type => 'done', id => $packet->{id}});
      }

      next;
    }

    foreach my $fh (@ready) {
      my $machine = first { $children->{$_}{r} == $fh } keys %$children;

      my $packet = read_packet($fh);

      if ($packet->{type} eq 'done') {
	my $done_id = $packet->{id};
	if (exists $left{$done_id}) {
	  delete $left{$done_id}{$machine};
	} else {
	  debug("Something is wrong with $done_id on " . Dumper($self->{hostname}, \%left));
	}
      } elsif (grep { $packet->{type} eq $_ } qw( debug failed_host result )) {
	put_packet(\*STDOUT, $packet);
      } else {
	$select->remove($fh);

	waitpid($children->{$machine}{pid}, 0);
	delete $children->{$machine};
	delete $left{$_}{$machine} for keys %left;

        put_packet(\*STDOUT, { type => 'failed_host', data => [@{$self->{hostname}}, $machine]});
      }
    }

    foreach my $id (keys %left) {
      if (! %{$left{$id}}) {
	put_packet(\*STDOUT, {type => 'done', id => $id});
	delete $left{$id};
      }
    }
  }
}

sub _recurse {
  my $self = shift;

  $SIG{PIPE} = 'IGNORE';

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

sub read_packet {
  my $fh = shift;

  my $packed_length = get($fh, 4);

  if (! defined $packed_length) {
    return {
      type => 'error',
      data => "couldn't read header",
    }
  }

  my $length = unpack("N", $packed_length);
  my $data   = get($fh, $length);

  if (! defined $data) {
    return {
      type => 'error',
      data => "couldn't read data",
    }
  }

  my $packet;
  eval $data;

  return $packet;
}

sub put_packet {
  my ($fh, $packet) = @_;

  my $payload = Data::Dumper->new([$packet],['packet'])->Deparse(1)->Dump();

  my $length = length($payload);

  put($fh, pack("N", $length) . $payload);
}

sub debug {
  my $string = shift;

  put_packet(\*STDOUT, {type => 'debug', data => $string});
}

sub program {
  my ($self, $machine) = @_;

  my $program = join("\n",
    $self->{header},
    'RecursiveSSH::Remote->new(',
    Data::Dumper->new([{
      header        => $self->{header},
      find_children => $self->{find_children},
      data          => $self->{data},
      hostname      => [@{$self->{hostname}}, $machine],
    }])->Deparse(1)->Terse(1)->Dump(),
    ')->_recurse;',
  ) . "\n";

  return $program;
}

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

sub get {
  my ($fh, $length) = @_;

  my $read = 0;
  my $buf = '';

  do {
    my $r = sysread($fh, $buf, $length - $read, $read);

    if (! defined $r) {
#TODO what to do here?
      return undef;
    } elsif ($r == 0) {
#TODO where do we get this?
      return undef;
    } else {
      $read += $r;
    }
  } while ($read < $length);

  return $buf;
}

sub slave_invocation {
  my $length = shift;
  return 'perl -e \'$l = ' . $length . '; do { $rt = sysread(STDIN, $b, $l - $r, $r); $r += $rt} while ($r < $l); eval $b; $@ and print $@\'';
}

sub ssh_invocation {
  my ($machine, $length) = @_;

  my $slave = slave_invocation($length);
  $slave =~ s/\$/\\\$/g;

  return 'ssh -oBatchMode=yes -oStrictHostKeyChecking=no -A ' . $machine . ' "' . $slave . '"';

}

1;
