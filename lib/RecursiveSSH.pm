package RecursiveSSH;

use strict;
use warnings;

our $VERSION = 0.001;

=pod

=head1 NAME

RecursiveSSH

=head1 SYNOPSIS

my $rssh = RecursiveSSH->new({
  children => $sub  # called with the RecursiveSSH::Remote object
});

$rssh->connect;

$rssh->exec(
  $sub_to_run_remotely,
  $on_read_cb,
  $on_done_cb,
);

$rssh->exec(...); # this follows the first

$rssh->loop; # consume all output from the last two execs

$rssh->exec(...);

$rssh->loop; # consume all output from the last exec

$rssh->quit;

=head1 DESCRIPTION

Provides a function for recursively traversing a network with ssh and agent
forwarding and then running commands on those remote nodes.  Largely
asynchronous, allows for the injection of multiple execs, in broadcast or with
destination, followed by a blocking collect.

The network shouldn't leave any garbage (zombie processes) if nodes die or if
processes hang.  It is also possible to shut the network down very rapidly if
needed (a multi-process model on each machine allows one process that
constantly waits for commands that can signal a second that carries out
execution).

=head1 METHODS

=over 4

=cut

use Data::Dumper;
use IPC::Open3;
use Sys::Hostname;
use Scalar::Util qw( refaddr );

use RecursiveSSH::Remote qw(
  read_packet
  put_packet
  slave_invocation
  put
);

END {
  clean_up();
}

my %_INSTANCES;

sub clean_up {
  foreach my $rssh (values %_INSTANCES) {
    $rssh->quit;
  }
}

=item $class->new({...})

Arguments to new are like this:
{
  children       => $recursive_sub,
  debug_cb       => $callback_for_debug_packets,
  failed_host_cb => $callback_for_failed_host_packets,

}

Provides an object which wraps the whole recurisve ssh tree.

=cut

sub new {
  my ($class, $info) = @_;
  my ($data, $graph, $children, $debug_cb, $failed_host_cb) = @{$info}{qw(
       data   graph   children   debug_cb   failed_host_cb)};

  $debug_cb ||= sub { warn shift };
  $failed_host_cb ||= sub { warn join("->", @{$_[0]})};

  my $header = do {
    open my $fh, $INC{'RecursiveSSH/Remote.pm'};
    local $/; 
    my $c = <$fh>;
    close $fh;
    $c;
  };

  if ($graph) {
    die "Graph must be a RecursiveSSH::Graph" unless $graph->isa("RecursiveSSH::Graph");

    if ($children) {
      die "Cannot have graph and children";
    }
    $data->{_graph} = $graph->for_data();

    $children = \&RecursiveSSH::Graph::graph_children;
  }

  my $string = join("\n",
    $header,
    Data::Dumper->new([{
      header        => $header,
      find_children => $children,
      data          => $data,
      hostname      => [hostname()],
    }], ['EVAL'])->Deparse(1)->Purity(1)->Dump,
    'RecursiveSSH::Remote->new($EVAL)->recurse;',
  );

  return bless {
    program        => $string,
    debug_cb       => $debug_cb,
    callbacks      => { },
    event_seq      => 0,
    failed_host_cb => $failed_host_cb,
  }, $class;
}

=item $self->connect()

Starts the recursion and brings up the network.

=cut

sub connect {
  my $self = shift;

  $self->{pid} and return;

  my $length = length($self->{program});

  my $pid = open3($self->{in}, $self->{out}, $self->{out}, slave_invocation($length));
  put($self->{in}, $self->{program});

  $self->{pid} = $pid;

  $_INSTANCES{refaddr($self)} = $self;

  $self->{events}++;

  return;
}

=item $self->loop()

Runs until all outstanding execs have finished.  This is blocking

=cut

sub loop {
  my $self = shift;

  while (%{$self->{callbacks}}) {
    $self->_read;
  }
}

sub _read {
  my $self = shift;

  $self->{pid} or die "Not running";

  if (my $packet = read_packet($self->{out})) {
    my $id = $packet->{id};

    if ($packet->{type} eq 'debug') {
      $self->{debug_cb}->($packet->{data});
    } elsif ($packet->{type} eq 'failed_host') {
      $self->{failed_host_cb}->($packet->{data});
    } elsif ($packet->{type} eq 'done') {
      $self->{callbacks}->{$id}->{on_done}->() if $self->{callbacks}->{$id}->{on_done};
      delete($self->{callbacks}->{$id});
    } elsif ($packet->{type} eq 'result') {
      $self->{callbacks}->{$id}->{on_read}->($packet->{data}) if $self->{callbacks}->{$id}->{on_read};
    } else {
      warn Dumper($packet);
      die "Unknown packet type";
    }
  } else {
    die "Couldn't get a packet back from read_packet";
  }

  return;
}

=item $self->quit

Closes down the network.  This is reasonably graceful.

=cut

sub quit {
  my $self = shift;

  $self->{pid} or return;

  put_packet($self->{in}, { type => 'quit' });
  waitpid($self->{pid}, 0);

  delete($self->{$_}) for qw( pid in out );

  delete $_INSTANCES{refaddr($self)};
}

=item $self->exec(...)

Same as exec_on, only broadcasts to all nodes in the network

See exec_on

=cut

sub exec {
  my $self = shift;

  $self->exec_on('broadcast', @_);

  return;
}

=item $self->exec_on($dest, $sub, $read_cb, $done_cb)

Runs $sub on all machines noted by $dest.  Calls $read_cb per response and
$done_cb when no more responses are coming.

$dest is like this:
[
  [ 'path', 'to', 'machine' ],
  [ 'path', 'to', 'machine2' ],
]

$sub receives the RecursiveSSH::Remote object as it's argument

=cut

sub exec_on {
  my ($self, $dest, $sub, $read_sub, $done_sub) = @_;

  $self->{pid} or die "Not running";

  put_packet($self->{in}, { type => 'exec', data => $sub, id => $self->{event_seq}, src => [], dest => $dest });

  $self->{callbacks}->{$self->{event_seq}} = {
    on_read => $read_sub,
    on_done => $done_sub,
  };

  $self->{event_seq}++;

  return;
}

sub DESTROY {
  my $self = shift;

  $self->quit;
}

1;

=pod

=back

=head1 AUTHOR

Jason Carey

=head1 SEE ALSO

L<RecursiveSSH::Graph>, L<RecursiveSSH::Remote>

=cut
