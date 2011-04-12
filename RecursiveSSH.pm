package RecursiveSSH;

use strict;
use warnings;
no warnings 'redefine';

use Data::Dumper;
use FileHandle;
use IPC::Open3;
use Sys::Hostname;

$SIG{PIPE} = 'IGNORE';
$SIG{INT} = 'IGNORE';

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

    my $program = program($machine);

    my $length = length($program);

    my $cmd = 'ssh -T -oBatchMode=yes -oStrictHostKeyChecking=no -A ' . $machine . ' "perl -e \'\\$l = ' . $length . '; do { \\$rt = sysread(STDIN, \\$b, \\$l - \\$r, \\$r); \\$r += \\$rt} while (\\$r < \\$l); eval \\$b; \\$@ and print \\$@\'"';

    $pids[$i] = open3($writers[$i], $readers[$i], $readers[$i], $cmd);

    put($writers[$i], $program);
  }

  my $parent_pid = $$;

  if (my $pid = fork) {
    $SIG{HUP} = sub {
      put($_, 'c') for @writers;

      waitpid($_, 0) for @pids;

      exit 1;
    };

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

      waitpid($pids[$i], 0);
      my $child_exit_status = $? >> 8;
      $output[$i] = "CHILD EXIT STATUS for $children[$i]: $child_exit_status\n" . $output[$i] if $child_exit_status;
    }

    print @output;

    kill 1, $pid;
    waitpid($pid, 0);

    exit 0;
  } else {
    my $sub = sub {
      kill 1, $parent_pid;
      exit 1;
    };

    $SIG{HUP} = sub {
      exit 0;
    };

    my $command = '';
    while (sysread(STDIN, $command, 1) > 0) {
      $sub->();
      $command = '';
    }
    exit 0;
  }
}

sub program {
  my $machine = shift;

  my $program = join("\n",
    $data->{header},
    Data::Dumper->new([$data],["RecursiveSSH::data"])->Deparse(1)->Dump,
    Data::Dumper->new([[@$hostname, $machine]], ["RecursiveSSH::hostname"])->Dump,
    'recurse',
  ) . "\n";

  return $program;
}

sub put {
  my ($fh, $string) = @_;

  my $length = length($string);

  my $wrote = 0;
  my $counter = 0;
  do {
    my $w = syswrite($fh, $string, $length - $wrote, $wrote);
    if (! defined $w) {
      return;
    } elsif ($w == 0) {
      sleep 1;
      return if $counter++ > 5;
    }

    $wrote += $w;
  } while ($wrote < $length);
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
