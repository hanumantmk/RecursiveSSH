package RecursiveSSH::Graph;

use strict;
use warnings;

our $VERSION = 0.001;

=pod

=head1 NAME

RecursiveSSH::Graph

=head1 SYNOPSIS

my $graph = RecursiveSSH::Graph->new({
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

=head1 METHODS

=over 4

=cut

use Sys::Hostname;

use Graph;

=item $class->new({})

Arguments like:
{
  info => {
    $node_name => $actual_address_to_ssh_to
  },
  edges => [
    [ $node_or_arrayref_of_nodes => $cost, $node_or_arrayref_of_nodes ],
    [ foo => 3, "bar" ],
    [ [ "bar", "baz" ] => 5, "bop" ],
    [ bop => 10, ["zop", "zap"]],
  ],
}

Returns back a graph object which can be passed to RecursiveSSH in place of a
children function.

=cut

sub new {
  my ($class, $options) = @_;

  my $self = bless $options, $class;

  $self->init_graph($options->{source} || hostname());

  return $self;
}

=item $self->dest_for_vertex($vertex)

Given a vertex (machine name), provide a dest pattern for it.  This will be the
path from the source to that node (it's RecursiveSSH addressing).  Optionally a
source other than the graph source can be passed as a second parameter

=cut

sub dest_for_vertex {
  my ($self, $vertex, $source) = @_;

  $source ||= $self->{source};

  $source = $self->{info}{$source};
  $vertex = $self->{info}{$vertex};

  my @path = $self->{graph}->SP_Bellman_Ford($source, $vertex);

  if (!@path) {
    die "unknown vertex: $vertex";
  }

  return \@path;
}

sub init_graph {
  my ($self, $source) = @_;

  my $graph = Graph->new();

  foreach my $edge (@{$self->{edges}}) {
    $self->process_edge($graph, @$edge);
  }

  foreach my $label (keys %{$self->{info}}) {
      if ($self->{info}{$label} eq $source) {
          $self->{source} = $label;
      }
  }

  $self->{source} ||= $source;

  my $spt = $graph->SPT_Dijkstra($source);

  $self->{graph} = $spt;

  return;
}

=item $self->for_data()

Returns a simplified single source shortest subtree for RecursiveSSH.  Also a
good debugging tool to see what the graph looks like.

=cut

sub for_data {
  my ($self, $vertex) = @_;

  $vertex ||= $self->{info}{$self->{source}};

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
      $self->{info}{$node} = $node;
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

=back

=head1 AUTHOR

Jason Carey

=head1 SEE ALSO

L<RecursiveSSH>, L<Graph>

=cut
