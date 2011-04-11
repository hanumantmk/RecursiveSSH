package RecursiveSSH;

use strict;
use warnings;
no warnings 'redefine';

use Data::Dumper;
use FileHandle;
use IPC::Open2;
use Sys::Hostname;
use IO::Handle;

$SIG{PIPE} = 'IGNORE';

our $data;
our $hostname = [];

sub recurse {
  my (@pids, @readers, @writers);

  my %hosts = map { $_, 1 } @$hostname;

  my @children = eval {
    grep { ! $hosts{$_} } $data->{children}->($data->{data});
  };
  print "Error in children: $@" if $@;

  for (my $i = 0; $i < @children; $i++) {
    my $machine = $children[$i];

    eval {
      if (my $user = $data->{users}->($data->{data}, $machine)) {
	$machine = join('@', $user, $machine);
      }
    };
    if ($@) {
      print "Error in users: $@";
      next;
    }

    my $cmd = 'ssh -oBatchMode=yes -oStrictHostKeyChecking=no -A ' . $machine . ' "perl -e \'eval do {undef local $/; <STDIN>};\\$@ and print \\$@\'"';

    $pids[$i] = open2($readers[$i], $writers[$i], $cmd);

    my $writer = $writers[$i];
    print $writer program($machine);
    close $writer;
  }

  eval {
    print $data->{run}->($data->{data});
  };
  print "Error in run: $@" if $@;

  my @output;
  for (my $i = 0; $i < @children; $i++) {
    my $reader = $readers[$i];
    while (my $line = <$reader>) {
      $output[$i] .= $line;
    }

    close $reader;

    waitpid($pids[$i], 0);
    my $child_exit_status = $? >> 8;
    $output[$i] = "CHILD EXIT STATUS for $children[$i]: $child_exit_status\n" . $output[$i] if $child_exit_status;
  }

  print @output;
  exit 0;
}

sub program {
  my $machine = shift;

  my $program = join("\n",
    $data->{header},
    Data::Dumper->new([$data],["RecursiveSSH::data"])->Deparse(1)->Dump,
    Data::Dumper->new([[@$hostname, $machine]], ["RecursiveSSH::hostname"])->Dump,
    'recurse',
  );

  return $program;
}

sub bootstrap {
  my %info = @_;
  my ($data, $children, $run, $users) = @info{qw(
       data   children   run   users)};

  $users ||= sub {};

  my $header = do {
    open my $fh, __FILE__;
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
    }],["RecursiveSSH::data"])->Deparse(1)->Dump,
    '$hostname = [qw(' . hostname() . ')];',
    'recurse',
  );

  eval $string;
  $@ and die $@;
}

1;
