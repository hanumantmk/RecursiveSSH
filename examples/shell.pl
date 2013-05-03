#!/usr/bin/perl -w

use RecursiveSSH;
use RecursiveSSH::Graph;
use RecursiveSSH::Shell;

use Getopt::Long;

my $graph_file;

GetOptions(
  'graph=s' => \$graph_file,
  'help|?'  => sub {HELP(0)},
);

$graph_file or print "Please provide a graph file\n" and HELP(1);

$SIG{INT} = \&RecursiveSSH::clean_up;

my $graph = RecursiveSSH::Graph->new(do {
  open my $fh, $graph_file or die "Couldn't open $graph_file: $!";

  local $/; 

  my $d = <$fh>;

  close $fh or die "Couldn't close $graph_file: $!";

  my $e = eval $d;
  $@ and die $@;

  $e
});

my $rssh = RecursiveSSH->new({
  graph => $graph,
  failed_host_cb => sub { print "FAILED HOST: " . join('->', @{$_[0]}) . "\n" }
});

$rssh->connect;

my $shell = RecursiveSSH::Shell->new($rssh, $graph);

$shell->cmdloop();

$rssh->loop;

exit 0;

sub HELP {
  my $exit = shift;

  print <<HELP
USAGE: $0 [options] machine1 [machine2 ...]

OPTIONS:
--graph A graph file to sprawl out and run on.  Eval'd in as the arguments to
        RecursiveSSH::Graph

--help  this help message
HELP
;

  exit $exit;
}
