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
  my ($data, $children, $users) = @{$info}{qw(
       data   children   users)};

  $users ||= sub {};

  my $header = do {
    open my $fh, $INC{'RecursiveSSH/Remote.pm'};
    local $/; 
    my $c = <$fh>;
    close $fh;
    $c;
  };

  my $string = join("\n",
    $header,
    Data::Dumper->new([{
      header   => $header,
      children => $children,
      data     => $data,
      users    => $users,
    }],["data"])->Deparse(1)->Dump,
    '$hostname = [qw(' . hostname() . ')];',
    'recurse',
  );

  return bless {
    program => $string,
    queue   => [],
    hosts   => [],
    failed_hosts => [],
    events  => 0,
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

sub _read {
  my $self = shift;

  return unless ($self->{events});

  $self->{pid} or die "Not running";

  while ($self->{events}) {
    my $packet = read_packet($self->{out});

    warn "in _read " . Dumper($packet) if $ENV{DEBUG};

    if ($packet->{type} eq 'data') {
      push @{$self->{queue}}, $packet->{data};
    } elsif ($packet->{type} eq 'host') {
      push @{$self->{hosts}}, $packet->{data};
    } elsif ($packet->{type} eq 'failed_host') {
      push @{$self->{failed_hosts}}, $packet->{data};
    } elsif ($packet->{type} eq 'done') {
      $self->{events}--;
    } else {
      warn Dumper($packet);
      die "Shouldn't be here";
    }
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

sub hosts {
  my $self = shift;

  $self->_read;

  return @{$self->{hosts}};
}

sub failed_hosts {
  my $self = shift;

  $self->_read;

  return @{$self->{failed_hosts}};
}

sub exec {
  my ($self, $sub) = @_;

  $self->{pid} or die "Not running";

  $self->{events}++;

  put_packet($self->{in}, { type => 'exec', data => $sub });

  $self->_read;

  my $queue = delete $self->{queue};
  $self->{queue} = [];

  return join('', @$queue);
}

sub DESTROY {
  my $self = shift;

  $self->quit;
}

1;
