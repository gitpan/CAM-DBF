use ExtUtils::MakeMaker;
WriteMakefile(
              'NAME'		=> 'CAM::DBF',
              'VERSION_FROM'	=> 'lib/CAM/DBF.pm', # finds $VERSION
              'PREREQ_PM'		=> {
                 'Getopt::Long'             => '2.0',    # needed for dbf2csv
                 'Pod::Usage'               => '1.0',    # needed for dbf2csv
                 'FileHandle'               => '2.0',
                 'Test::More'               => '0.01',
              },
              'EXE_FILES'         => [
                                      "bin/dbf2csv",
                                      "bin/packdbf",
                                      "bin/debugdbf",
                                      ],
              'PL_FILES'        => {},
              ($] >= 5.005 ?
               (ABSTRACT_FROM => 'lib/CAM/DBF.pm',
                AUTHOR     => 'Clotho Advanced Media <cpan@clotho.com>') : ()),

              );
