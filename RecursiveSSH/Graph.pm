package RecursiveSSH::Graph;

use strict;
use warnings;

use Sys::Hostname;

use Graph;

sub new {
  my ($class, $options) = @_;

  $options->{source} ||= hostname();

  my $self = bless $options, $class;

  $self->init_graph;

  return $self;
}

sub dest_for_vertex {
  my ($self, $vertex) = @_;

  $vertex = exists $self->{info}{$vertex} ? $self->{info}{$vertex} : $vertex;

  my @path = $self->{graph}->SP_Bellman_Ford($self->{source}, $vertex);

  if (!@path) {
    die "unknown vertex: $vertex";
  }

  return \@path;
}

sub init_graph {
  my $self = shift;

  my $graph = Graph->new();

  foreach my $edge (@{$self->{edges}}) {
    $self->process_edge($graph, @$edge);
  }

  my $spt = $graph->SPT_Dijkstra($self->{source});

  $self->{graph} = $spt;

  return;
}

sub for_data {
  my ($self, $vertex) = @_;

  $vertex ||= $self->{source};

  my $info = $self->{info};

  return {
    node     => $vertex,
    children => [
      map {
	$self->for_data($_)
      } $self->{graph}->successors($vertex)
    ],
  }
}

sub process_node {
  my ($self, $node) = @_;

  if (ref $node eq 'ARRAY') {
    map { $self->process_node($_) } @$node;
  } else {
    if (exists $self->{info}{$node}) {
      $self->{info}{$node};
    } else {
      $node;
    }
  }
}

sub process_edge {
  my ($self, $graph, $src, $cost, $dest) = @_;

  my ($snodes, $enodes) = map { [$self->process_node($_)] } ($src, $dest);

  foreach my $snode (@$snodes) {
    foreach my $enode (@$enodes) {
      next if $snode eq $enode;
      $graph->add_weighted_edge($snode, $enode, $cost);
    }
  }
}

sub graph_children {
  my $self = shift;

  my $data = $self->data;

  my $hostname = $self->hostname;

  my @children = @{$data->{_graph}{children}};

  return () if ! @children;

  if (scalar(@$hostname) > 1) {
    my $last_hop = $hostname->[-1];

    for (my $i = 0; $i < scalar(@children); $i++) {
      if ($children[$i]->{node} eq $last_hop) {
	$data->{_graph} = $children[$i];
	last;
      }
    }
  }

  return map { $_->{node} } @{$data->{_graph}{children}};
}

1;

=pod

=head1 NAME

RecursiveSSH::Graph

=head1 SYNOPSIS

my $graph = RecursiveSSH::Graph({
  info => {
    $node_name => $actual_address_to_ssh_to
  },
  edges => [
    [ $node_or_arrayref_of_nodes => $cost, $node_or_arrayref_of_nodes ],
    [ foo => 3, "bar" ],
    [ [ "bar", "baz" ] => 5, "bop" ],
    [ bop => 10, ["zop", "zap"]],
  ],
});

my $r = RecursiveSSH->new({graph => $graph});

my $dest = $graph->dest_for_vertex("foo");

$r->exec_on($dest, ...);

=head1 DESCRIPTION

Supplies RecursiveSSH with a child function that will traverse a single source
shortest path subtree given the current hostname and a set of edges.  Provides
a few utility functions such as dest_for_vertex for use with these graphs.

Uses Graph for the graph functions (SPT_Dijkstra and SP_Bellman_Ford).

=head1 AUTHOR

Jason Carey

=head1 SEE ALSO

L<RecursiveSSH>, L<Graph>

=cut
