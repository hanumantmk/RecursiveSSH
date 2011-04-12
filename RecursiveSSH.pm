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
  my ($data, $children, $run, $users) = @{$info}{qw(
       data   children   run   users)};

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
      run      => $run,
      data     => $data,
      users    => $users,
    }],["data"])->Deparse(1)->Dump,
    '$hostname = [qw(' . hostname() . ')];',
    'recurse',
  );

  return bless {
    program => $string,
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

  return;
}

sub read {
  my $self = shift;

  $self->{pid} or die "Not running";

  my $packet = read_packet($self->{out});

  if ($packet->{type} eq 'data') {
    return $packet->{data};
  } elsif ($packet->{type} eq 'done') {
    return undef;
  } else {
    warn Dumper($packet);
    die "Shouldn't be here";
  }
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
  my ($self, $sub) = @_;

  $self->{pid} or die "Not running";

  put_packet($self->{in}, { type => 'exec', data => $sub });
}

sub DESTROY {
  my $self = shift;

  $self->quit;
}

1;
