package RecursiveSSH::Shell;

use RecursiveSSH;
use RecursiveSSH::Graph;

use base 'Term::Shell';

sub catch_run {
    my ($o, @stuff) = @_;

    print join("", $o->run_on_ctx( sub { my ($self, @stuff) = @_; my @x = `@stuff 2>&1`; @x}, @stuff));
}

sub comp_ {
    my ($o, $word, $line, $start) = @_;

    sort $o->run_on_ctx(sub {
        my ($self, $word) = @_;

        my @lines = `bash -c 'compgen -A command $word'`;
        chomp for @lines;

        return @lines;
    }, $word);
}

sub catch_comp {
    my $o = shift;

    my $start = pop;
    my $line = pop;

    my @words = @_;

    sort $o->run_on_ctx(
      sub {
        my ($self, $line, @words) = @_;

        my $command = 'bash -c "source /etc/bash_completion; complete -p"';

        my %bash_completedb = (
            map {
                chomp;
                @_ = split /\s+/, $_;

                shift;
                my $command = pop;

                my $func;

                for (my $i = 0; $i < @_; $i++) {
                    if ($_[$i] eq '-F') {
                        $func = $_[$i+1];
                        last;
                    }
                }

                $func ? ($command, $func) : ()
            } `$command`
        );

        my $count = length($line) + 1;
        my $cword = scalar(@words) - 1;
        my $start = length($line);

        if (my $func = $bash_completedb{$words[0]}) {
            my $command = 'bash -c \'
            source /etc/bash_completion;
            __print_completions() {
                for ((i=0;i<${#COMPREPLY[*]};i++));
                do
                   echo ${COMPREPLY[i]};
                done;
            };' . "
            COMP_WORDS=($line);
            COMP_LINE=\"$line\";
            COMP_COUNT=$count;
            COMP_CWORD=$cword;
            COMP_POINT=$start;
            $func;
            __print_completions;'";

            $command =~ s/\n//g;

            my @lines = `$command`;

            chomp for @lines;
            return @lines;
        } else {
            return ()
        }
      },
      $line, @words,
    );
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

    sort grep {
        $_ =~ /^$word/i
    } keys %{$o->{SHELL}{graph}{info}};
}

sub run_cd {
    my ($o, $dir) = @_;

    my ($r) = $o->run_on_ctx(
      sub { my ($self, $dir) = @_; $dir ||= $ENV{HOME}; chdir $dir },
      $dir
    );
    
    $r or print "cd: $dir: No such file or directory\n";
}

sub comp_cd {
    my ($o, $word, $line, $start) = @_;

    sort $o->run_on_ctx(
        sub { my ($self, $word) = @_; glob "$word*" },
        $word
    );
}

sub prompt_str {
    my $o = shift;

    return $o->{SHELL}{ctx} . "> ";
}

sub run_on_ctx {
    my ($o, $fun, @args) = @_;

    my @out;

    $o->{SHELL}{rssh}->exec_on(
      [ $o->{SHELL}{graph}->dest_for_vertex($o->{SHELL}{ctx}) ],
      $fun,
      \@args,
      sub {
        push @out, @_;
      },
      sub { },
    );

    $o->{SHELL}{rssh}->loop();

    return @out;
}

1;
