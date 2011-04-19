package RecursiveSSH;

use strict;
use warnings;

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

sub new {
  my ($class, $info) = @_;
  my ($data, $children, $debug_cb, $failed_host_cb) = @{$info}{qw(
       data   children   debug_cb   failed_host_cb)};

  $debug_cb ||= sub { warn shift };
  $failed_host_cb ||= sub { warn shift };

  my $header = do {
    open my $fh, $INC{'RecursiveSSH/Remote.pm'};
    local $/; 
    my $c = <$fh>;
    close $fh;
    $c;
  };

  my $string = join("\n",
    $header,
    'RecursiveSSH::Remote->new(',
    Data::Dumper->new([{
      header        => $header,
      find_children => $children,
      data          => $data,
      hostname      => [hostname()],
    }])->Deparse(1)->Terse(1)->Dump,
    ')->_recurse;',
  );

  return bless {
    program        => $string,
    debug_cb       => $debug_cb,
    callbacks      => { },
    event_seq      => 0,
    failed_host_cb => $failed_host_cb,
  }, $class;
}

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

sub quit {
  my $self = shift;

  $self->{pid} or return;

  put_packet($self->{in}, { type => 'quit' });
  waitpid($self->{pid}, 0);

  delete($self->{$_}) for qw( pid in out );

  delete $_INSTANCES{refaddr($self)};
}

sub exec {
  my ($self, $sub, $read_sub, $done_sub) = @_;

  $self->{pid} or die "Not running";

  put_packet($self->{in}, { type => 'exec', data => $sub, id => $self->{event_seq} });

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
