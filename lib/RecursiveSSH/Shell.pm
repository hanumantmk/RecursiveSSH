package RecursiveSSH::Shell;

use RecursiveSSH;
use RecursiveSSH::Graph;

use base 'Term::Shell';

sub catch_run {
    my ($o, @stuff) = @_;

    print join("", $o->magic_pipe($o->line()));
#    print join("", $o->run_on_ctx( sub { my ($self, @stuff) = @_; my @x = `@stuff 2>&1`; @x}, @stuff));
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

sub magic_pipe {
    my ($o, $line) = @_;

    my @commands = map {
        my $ctx = $o->{SHELL}{ctx};
        my $cmd = $_;

        if (/^\s*!\s*(\S+)\s+(.+)/) {
            $ctx = $1;
            $cmd = $2;
        }

        [$ctx, $cmd];
    } split /@\|@/, $line;

    my $r;

    for (my $i = 0; $i < @commands; $i++) {
        my ($ctx, $cmd) = @{$commands[$i]};

        if ($i == 0) {
           ($r) = $o->run_on($ctx, sub { my ($self, $cmd) = @_; return scalar `$cmd` }, $cmd);
        } else {
           ($r) = $o->run_on($ctx, sub {
               my ($self, $cmd, $data) = @_;

               my ($in, $out);

               my $pid = IPC::Open2::open2($out, $in, $cmd);

               my $s = IO::Select->new();

               $s->add($in, $out);

               my $written = 0;
               my $read = 0;
               my $buf = '';

               LOOP: while (1) {
                   while (length($data) > $written && $s->can_write) {
                       $written += syswrite($in, $data, length($data) - $written, $written);
                   }

                   if (length($data) == $written) {
                       $s->remove($in);
                       close $in;
                   }

                   while ($s->can_read) {
                       my $r = sysread($out, $buf, 4096, $read);

                       if ($r > 0) {
                           $read += $r;
                       } else {
                           last LOOP;
                       }
                   }
               }

               waitpid( $pid, 0 );

               return $buf;
           }, $cmd, $r);
        }
    }

    return $r;
}

sub run_magic_cp {
    my ($o, $from, $to) = @_;

    my ($from_machine, $from_path) = split /:/, $from, 2;
    my ($to_machine, $to_path) = split /:/, $to, 2;

    $o->magic_pipe("!$from_machine tar -cf - $from_path @|@ !$to_machine tar -xpf -");
}

sub prompt_str {
    my $o = shift;

    return $o->{SHELL}{ctx} . "> ";
}

sub run_on {
    my ($o, $ctx, $fun, @args) = @_;

    my @out;

    $o->{SHELL}{rssh}->exec_on(
      [ $o->{SHELL}{graph}->dest_for_vertex($ctx) ],
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

sub run_on_ctx {
    my ($o, $fun, @args) = @_;

    return $o->run_on($o->{SHELL}{ctx}, $fun, @args);
}

1;
