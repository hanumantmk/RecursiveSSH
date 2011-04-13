package RecursiveSSH::Remote;

use strict;
use warnings;

use Data::Dumper;
use IPC::Open3;
use IO::Select;

our $data;
our $hostname = [];

use base 'Exporter';

our @EXPORT_OK = qw(
  read_packet
  put_packet
  slave_invocation
  put
);

sub recurse {
  $SIG{PIPE} = 'IGNORE';

  put_packet(\*STDOUT, { type => 'host', data => $hostname });

  my $child_fh;

  my $pid = open $child_fh, "|-";
  if (! defined $pid) {
    print_up("Couldn't fork: $!");
  }

  if ($pid) {
    $0 = 'RecursiveSSH Parent';

    my @queue;
    my $kill_sub = sub {
      kill 1, $pid;
      exit 0;
    };

    my $jump_table = {
      'quit' => $kill_sub,
      'exec' => sub {
	my $packet = shift;

	push @queue, $packet;
      },
      'error' => sub {
	print_up("Error packet, dunno");
	$kill_sub->();
      },
      'data' => sub {
	print_up("We shouldn't be passing data packets up");
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
	  print_up("garbage on STDIN");
	  $kill_sub->();
	}
      } elsif (@$writing && @queue) {
	put_packet($child_fh, shift @queue);
      }
    }

    $kill_sub->();
  }
  $0 = 'RecursiveSSH Child';

  my (@pids, @readers, @writers);

  my %hosts = map { $_, 1 } @$hostname;

  my @children = eval {
    grep { ! $hosts{$_} } $data->{children}->($data->{data});
  };
  print_up("Error in children: $@") if $@;

  for (my $i = 0; $i < @children; $i++) {
    my $machine = $children[$i];

    eval {
      if (my $user = $data->{users}->($data->{data}, $machine)) {
	$machine = join('@', $user, $machine);
      }
    };
    if ($@) {
      print_up("Error in users: $@");
      next;
    }

    my $program = program($machine);

    my $length = length($program);

    my $cmd = ssh_invocation($machine, $length);

    $pids[$i] = open3($writers[$i], $readers[$i], $readers[$i], $cmd);

    put($writers[$i], $program);
  }

  my $end_sub = $SIG{HUP} = sub {
    put_packet($_, {type => 'quit'}) for @writers;

    waitpid($_, 0) for @pids;

    exit 0;
  };

  my $run_id = 0;

  my %left = ($run_id, scalar(@children));

  if (! @readers) {
    put_packet(\*STDOUT, {type => 'done', data => $run_id});
  }

  my $select = IO::Select->new(\*STDIN, @readers);

  while (1) {
    my @ready = $select->can_read();

    if ($ready[0] == \*STDIN) {
      shift @ready;

      my $packet = read_packet(\*STDIN);

      if ($packet->{type} ne 'exec') {
	print_up("Bad exec packet");
	$end_sub->();
      }
      $run_id++;
      $left{$run_id} = scalar(@children);

      put_packet($_, $packet) for @writers;

      eval { $packet->{data}->($data->{data}) };
      print_up($@) if $@;

      if (! @readers) {
	put_packet(\*STDOUT, {type => 'done', data => $run_id});
      }

      next;
    }

    foreach my $fh (@ready) {
      my $packet = read_packet($fh);

      if ($packet->{type} eq 'done') {
	my $done_id = $packet->{data};
	if (exists $left{$done_id}) {
	  $left{$done_id}--;
	} else {
	  print_up("Something is wrong with $done_id on " . Dumper($hostname, \%left));
	}
      } elsif ($packet->{type} eq 'data' || $packet->{type} eq 'host' || $packet->{type} eq 'failed_host') {
	put_packet(\*STDOUT, $packet);
      } else {
	my $i;
	for ($i = 0; $i < scalar(@readers); $i++) {
	  $fh == $readers[$i] and last;
	}

	$select->remove($fh);

	my $machine = splice @children, $i, 1;
	my $pid     = splice @pids, $i, 1;
	waitpid($pid, 0);
	splice @readers, $i, 1;
	splice @writers, $i, 1;

	$left{$_}-- for keys %left;

        put_packet(\*STDOUT, { type => 'failed_host', data => [@$hostname, $machine]});
      }
    }

    foreach my $i (keys %left) {
      if ($left{$i} <= 0) {
	put_packet(\*STDOUT, {type => 'done', data => $i});
	delete $left{$i};
      }
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

sub print_up {
  my $string = shift;

  put_packet(\*STDOUT, {type => 'data', data => $string});
}

sub program {
  my $machine = shift;

  my $program = join("\n",
    $data->{header},
    Data::Dumper->new([$data],["data"])->Deparse(1)->Dump,
    Data::Dumper->new([[@$hostname, $machine]], ["hostname"])->Dump,
    'recurse',
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
