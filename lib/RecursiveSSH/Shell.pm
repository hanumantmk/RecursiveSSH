package RecursiveSSH::Shell;

use RecursiveSSH;
use RecursiveSSH::Graph;

use base 'Term::Shell';

sub catch_run {
    my ($o, @stuff) = @_;

    $o->{SHELL}{rssh}->exec_on(
      [ $o->{SHELL}{graph}->dest_for_vertex($o->{SHELL}{ctx}) ],
      sub { my ($self, @stuff) = @_; `@stuff` },
      \@stuff,
      sub {
        print @_;
      },
      sub { },
    );

    $o->{SHELL}{rssh}->loop();
}

sub init {
    my $o = shift;

    my ($rssh, $graph) = @{$o->{API}{args}};

    $o->{SHELL} = {
        rssh  => $rssh,
        graph => $graph,
        ctx   => $graph->{source},
    };
}

sub run_switch {
    my ($o, $machine) = @_;

    $o->{SHELL}{ctx} = $machine;
}

sub comp_switch {
    my ($o, $word, $line, $start) = @_;

    grep {
        $_ =~ /^$word/i
    } sort keys %{$o->{SHELL}{graph}{info}};
}

sub prompt_str {
    my $o = shift;

    return $o->{SHELL}{ctx} . "> ";
}

1;
