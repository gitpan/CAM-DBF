#!/usr/bin/perl -w

use warnings;
use strict;
use Carp;
$SIG{__WARN__} = $SIG{__DIE__} = \&confess;


BEGIN
{
   use Test::More tests => 15;
   use_ok("CAM::DBF");
}

my ($start,$stop); # used for time testing

# Make a temp file in the same directory as this testfile
my $tmpfile = __FILE__;
$tmpfile =~ s,[^/]*$,test.dbf,;

my @columns = (
               {name=>"id",
                type=>"N", length=>8,  decimals=>0},
               {name=>"lastedit",
                type=>"D", length=>8,  decimals=>0},
               {name=>"firstname",
                type=>"C", length=>15, decimals=>0},
               {name=>"lastname",
                type=>"C", length=>20, decimals=>0},
               {name=>"height_cm",
                type=>"N", length=>6, decimals=>2},
               {name=>"active",
                type=>"L", length=>1, decimals=>0},
               );

my $mult = 10000;
my $num = 3777;

print "# Performance measurements are CPU time for $num rows written\n";
print "#   and ".($num*2)." rows read, extrapolated to time for $mult rows\n";
print "#   for readability\n";

{
   # Enclose in a block so $dbf goes away
   my $dbf = CAM::DBF->create($tmpfile, @columns);
   ok($dbf, "Create new dbf table");

   $start = &getTime();
   foreach my $i (0..$num-1)
   {
      $dbf->appendrow_arrayref([$i,"03/02/03","Clotho","Adv Media", 200, "Y"]);
   }
   $stop = &getTime();
   ok(1,"Performance of appendrow_arrayref: " . $mult*($stop-$start)/$num . " secs/$mult records");
   is($dbf->nrecords(), $num, "Count appended records");

   $start = &getTime();
   foreach my $i (0..$num-1)
   {
      $dbf->appendrow_hashref({id => $i+$num,
                               lastedit => "03/02/03",
                               firstname => "Clotho",
                               lastname => "Adv Media",
                               height_cm => 200,
                               active => "Y"});
   }
   $stop = &getTime();
   ok(1,"Performance of appendrow_hashref: " . $mult*($stop-$start)/$num . " secs/$mult records");

   is($dbf->nrecords(), $num*2, "Count appended records");

   ok ($dbf->closeDB(), "Close database after writing");
}

{
   my $dbf = CAM::DBF->new($tmpfile);
   ok($dbf, "Reopen dbf table");

   is($dbf->nrecords(), $num*2, "Count records");

   is_deeply($dbf->{fields}, \@columns, "Test column data structure");

   my $errors = 0;
   $start = &getTime();
   for my $iRow (0 .. $dbf->nrecords()-1)
   {
      my $ref = $dbf->fetchrow_arrayref($iRow);
      $errors++ if ((!$ref) || $ref->[0] != $iRow);
   }
   $stop = &getTime();
   ok(1,"Performance of fetchrow_arrayref: " . $mult*($stop-$start)/$dbf->nrecords() . " secs/$mult records");

   is ($errors, 0, "Test IDs for incoming rows");

   $errors = 0;
   $start = &getTime();
   for my $iRow (0 .. $dbf->nrecords()-1)
   {
      my $ref = $dbf->fetchrow_hashref($iRow);
      $errors++ if ((!$ref) || $ref->{id} != $iRow);
   }
   $stop = &getTime();
   ok(1,"Performance of fetchrow_hashref: " . $mult*($stop-$start)/$dbf->nrecords() . " secs/$mult records");

   is ($errors, 0, "Test IDs for incoming rows");
}

ok (unlink($tmpfile), "Delete test database");

sub getTime
{
   my($user,$system,$cuser,$csystem)=times;
   return $user+$system+$cuser+$csystem;
}
