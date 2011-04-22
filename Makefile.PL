use ExtUtils::MakeMaker;
use strict;
use warnings;

WriteMakefile(
  NAME             => 'RecursiveSSH',
  VERSION_FROM     => 'lib/RecursiveSSH.pm',
  ABSTRACT         => "Asynchronous Recursive SSH Framework",
  AUTHOR           => 'Jason Carey <jcarey@cpan.org>',
  MIN_PERL_VERSION => '5.8.4',
  LICENSE          => 'perl',

  PREREQ_PM => {
    'List::Util'    => 0,
    'Data::Dumper'  => 0,
    'IPC::Open3'    => 0,
    'Sys::Hostname' => 0,
    'Scalar::Util'  => 0,
    'IO::Select'    => 0,
  },
  META_MERGE => {
    recommends => {
      'Graph'        => '0.20',
      'Getopt::Long' => 0,
    },
    resources => {
      license => 'http://dev.perl.org/licenses',
    },
  },
);
