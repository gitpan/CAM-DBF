use Module::Build;
Module::Build->new(
                   module_name => 'CAM::DBF',
                   license => 'perl',
                   requires => {
                      'Getopt::Long'             => '2.0',    # needed for dbf2csv
                      'Pod::Usage'               => '1.0',    # needed for dbf2csv
                      'FileHandle'               => '2.0',
                   },
                   build_requires => {
                      'Test::More'               => '0.01',
                   },
                   script_files => [
                                    "bin/dbf2csv",
                                    "bin/packdbf",
                                    "bin/debugdbf",
                                    ],
                   )->create_build_script;
