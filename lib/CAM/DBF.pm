package CAM::DBF;

=head1 NAME

CAM::DBF - Perl extension for reading and writing dBASE III DBF files

=head1 LICENSE

Copyright 2005 Clotho Advanced Media, Inc., <cpan@clotho.com>

This library is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=head1 SEE ALSO

Please see the XBase modules on CPAN for more complete implementations
of DBF file reading and writing.  This module differs from those in
that it is designed to be error-correcting for corrupted DBF files,
and is (IMHO) simpler to use.

=head1 SYNOPSIS

  use CAM::DBF;
  my $dbf = new CAM::DBF($filename);
  
  # Read routines:
  
  print join("|", $dbf->fieldnames()),"\n";
  for my $row (0 .. $dbf->nrecords()-1) {
     print join("|", $dbf->fetchrow_array($row)),"\n";
  }
  
  my $row = 100;
  my $hashref = $dbf->fetchrow_hashref($row);
  my $arrayref = $dbf->fetchrow_hashref($row);
  
  # Write routines:
  
  $dbf->delete($row);
  $dbf->undelete($row);

=head1 DESCRIPTION

This package facilitates reading dBASE III PLUS DBF files.  This is
made possible by documentation generously released by Borland at:

  http://community.borland.com/article/0%2C1410%2C15838%2C00.html

Currently, only version III PLUS files are readable.  Support has not
yet been added for dBASE version IV or 5.0 files.

This library also supports writing dBASE files, but the writing
interface is not as polished as the reading interface.

=cut

require 5.005_62;
use strict;
use Carp;
use FileHandle;

our @ISA = qw();
our $VERSION = '1.01';

## Package globals

# Performance tests showed that a rowcache of 100 is better than
# rowcaches of 10 or 1000 (presumably due to tradeoffs in overhead
# vs. processor data cache usage vs. memory allocation)

our $ROWCACHE = 100;  # how many rows to cache at a time
#$ROWCACHE = 0;  # debugging

#----------------

=head1 CLASS METHODS

=over 4

=cut

#----------------

# Internal function, called by new() or create()

sub _init
{
   my $pkg = shift;
   my $filename = shift;
   my $filemode = shift;

   my %flags;
   %flags = (@_) if (@_ % 2 == 0);

   $filemode = "r" if ((!defined($filemode)) || $filemode eq "");

   my @times = localtime();
   my $year = $times[5]+1900;
   my $month = $times[4]+1;
   my $date = $times[3];

   my $self = bless {
      filename => $filename, 
      filemode => $filemode,
      fh => undef,
      fields => [],
      columns => [],

      valid => 0x03,
      year => $year,
      month => $month,
      date => $date,
      nrecords => 0,
      nheaderbytes => 0,
      nrecordbytes => 0,
      packformat => "C",

      flags => \%flags,
   }, $pkg;

   if ($filename eq "-")
   {
      # This might be fragile, since seek won't work
      if ($filemode =~ /r/)
      {
         $self->{fh} = FileHandle->new_from_fd(*STDIN, "r");
      }
      else
      {
         $self->{fh} = FileHandle->new_from_fd(*STDOUT, "w");
      }
   }
   else
   {
      $self->{fh} = new FileHandle $filename, $filemode;
   }
   if (!$self->{fh})
   {
      croak("Cannot open DBF file $filename: $!");
   }

   return $self;
}
#----------------

=item new FILENAME

=item new FILENAME, MODE

=item new FILENAME, MODE, KEY => VALUE, KEY => VALUE, ...

Open and read a dBASE file.  The optional mode parameter defaults to
"r" for readonly.  If you plan to append to the DBF, open it as "r+".

Additional behavior flags can be passed after the file mode.
Available flags are:

  ignoreHeaderBytes => 0|1 (default 0)
      looks for the 0x0D end-of-header marker instead of trusting the 
      stated header length
  allowOffByOne => 0|1 (default 0)
      only matters if ignoreHeaderBytes is on.  If the computed header
      size differs from the declared header size by one byte, use the
      latter.
  verbose => 0|1 (default 0)
      print warning messages about header problems, or stay quiet

=cut

sub new
{
   my $pkg = shift;
   my $filename = shift;
   my $filemode = shift;

   my $self = $pkg->_init($filename, $filemode, @_);

   ## Parse the header

   my $header;
   read($self->{fh}, $header, 32);
   ($self->{valid},
    $self->{year},
    $self->{month},
    $self->{date},
    $self->{nrecords},
    $self->{nheaderbytes},
    $self->{nrecordbytes}) = unpack "CCCCVvv", $header;
   
   unless ($self->{valid} && ($self->{valid} == 0x03 || $self->{valid} == 0x83))
   {
      croak("This does not appear to be a dBASE III PLUS file ($filename)");
   }

   my $filesize = ($self->{nheaderbytes} + 
                   $self->{nrecords}*$self->{nrecordbytes});
   $self->{filesize} = -s $filename;

   if ($self->{filesize} < $self->{nheaderbytes})
   {
      unless ($self->{flags}->{ignoreHeaderBytes})
      {
         croak("DBF file $filename appears to be severely truncated:\nHeader says it should be $filesize bytes, but it's only $self->{filesize} bytes\n  Records= $self->{nrecords}\n ");
      }
   }
   
   # correct 2 digit year
   $self->{year} += 1900;
   $self->{year} += 100 if ($self->{year} < 1970);  # Y2K fix

   my $field;
   my $pos = 64;
   read($self->{fh}, $field, 1);

   # acording to the Borland spec 0x0D marks the end of the header block
   # however we have seen this fail so $pos ensures we do not read beyond
   # the header block for table columns
   # We've also found flaky files which use 0x0A instead of 0x0D
   while ($field && (unpack("C", $field) != 0x0D) && (unpack("C", $field) != 0x0A) && 
          ($self->{flags}->{ignoreHeaderBytes} || 
           $pos < $self->{nheaderbytes}))
   {
      read($self->{fh}, $field, 31, 1);
      my ($name, $type, $junk1, $junk2, $junk3, $junk4, $len, $dec) = 
          unpack "a11a1CCCCCC", $field;

      $name =~ s/^(\w+).*?$/$1/s;
      
      push(@{$self->{fields}}, 
           {
              name => $name,
              type => $type,
              length => $len,
              decimals => $dec,
           });
      push @{$self->{columns}}, $name;

      $pos += 32;
      read($self->{fh}, $field, 1);
   }

   if ($self->{flags}->{ignoreHeaderBytes})
   {
      # replace stated header size with the actual, computed value
      my $oldvalue = $self->{nheaderbytes};
      my $newvalue = (@{$self->{fields}} + 1) * 32 + 1;
      # skip the replacement if the flags say to be lenient
      unless ($self->{flags}->{allowOffByOne} && abs($oldvalue-$newvalue) <= 1)
      {
         $self->{nheaderbytes} = $newvalue;
         if ($self->{flags}->{verbose} && $oldvalue != $self->{nheaderbytes})
         {
            warn("Corrected header size from $oldvalue to $$self{nheaderbytes} for $$self{filename}\n");
         }
      }
   }

   $self->{packformat} = "C";
   foreach my $field (@{$self->{fields}})
   {
      if ($field->{type} =~ /^[CLND]$/)
      {
         $self->{packformat} .= "a" . $field->{length};
      }
      else
      {
         croak("unrecognized field type ".$field->{type}." in field ".$field->{name});
      }
   }
   seek($self->{fh}, $self->{nheaderbytes}, 0);

   return $self;
}
#----------------

=item create FILENAME, [FLAGS,] COLUMN, COLUMN, ...

=item create FILENAME, FILEMODE, [FLAGS,] COLUMN, COLUMN, ...

Create a new DBF file in FILENAME, initially empty.  The optional
FILEMODE argument defaults to "w+".  We can't think of any reason to
use any other mode, but if you can think of one, go for it.

The column structure is specified as a list of hash references, each
containing the fields: name, type, length and decimals.  The name
should be 11 characters or shorted.  The type should be one of C, N,
D, or L (for character, number, date or logical).

The optional flags are:

  -quick => 0|1 (default 0) -- skips column format checking if set

Example:

   my $dbf = CAM::DBF->create("new.dbf",
                              {name=>"id",
                               type=>"N", length=>8,  decimals=>0},
                              {name=>"lastedit",
                               type=>"D", length=>8,  decimals=>0},
                              {name=>"firstname",
                               type=>"C", length=>15, decimals=>0},
                              {name=>"lastname",
                               type=>"C", length=>20, decimals=>0},
                              );

=cut

sub create
{
   my $pkg = shift;
   my $filename = shift;

   # Optional args:
   my $quick = 0;
   my $filemode = "w+";
   while (@_ > 0 && $_[0] && (!ref $_[0]))
   {
      if ($_[0] eq "-quick")
      {
         shift;
         $quick = shift;
      }
      elsif ($_[0] =~ /^[awr]\+?$/)
      {
         $filemode = shift;
      }
      else
      {
         &carp("Argument $_[0] not understood");
         return undef;
      }
   }

   # The rest of the args are the data structure definition
   my @columns = (@_);

   # Validate the column structure
   if ($quick)
   {
      if (!$pkg->validateColumns(@columns))
      {
         return undef;
      }
   }

   my $self = $pkg->_init($filename, $filemode);
   return undef if (!$self);

   $self->{fields} = [@columns];
   $self->{columns} = map {$_->{name}} @columns;
   $self->{packformat} = "C" . join("", map {"a".$_->{length}} @columns);

   if (!$self->writeHeader())
   {
      return undef;
   }

   return $self;
}
#----------------

=item validateColumns COLUMN, COLUMN, ...

Check an array of DBF columns structures for validity.  Emits warnings
and returns undef on failure.

=cut

sub validateColumns
{
   my $pkg_or_self = shift;
   my @columns = (@_);

   if (@columns == 0 && ref($pkg_or_self))
   {
      my $self = $pkg_or_self;
      @columns = @{$self->{fields}};
   }

   my $nColumns = 0; # used solely for error messages
   my %colNames;  # used to detect duplicate column names
   foreach my $column (@columns)
   {
      $nColumns++;
      if ((!$column) || (!ref $column) || ref($column) ne "HASH")
      {
         &carp("Column $nColumns is not a hash reference");
         return undef;
      }
      foreach my $key ("name", "type", "length", "decimals")
      {
         if ((!defined $column->{$key}) || $column->{$key} =~ /^\s*$/)
         {
            &carp("No $key field in column $nColumns");
            return undef;
         }
      }
      if (length($column->{name}) > 11)
      {
         &carp("Column name '$$column{name}' is too long (max 11 characters)");
         return undef;
      }
      if ($colNames{$column->{name}}++)
      {
         &carp("Duplicate column name '$$column{name}'");
         return undef;
      }
      if ($column->{type} !~ /^C|N|D|L$/)
      {
         &carp("Unknown column type '$$column{type}'");
         return undef;
      }
      if ($column->{length} !~ /^\d+$/)
      {
         &carp("Column length must be an integer ('$$column{length}')");
         return undef;
      }
      if ($column->{decimals} !~ /^\d+$/)
      {
         &carp("Column decimals must be an integer ('$$column{decimals}')");
         return undef;
      }
      if ($column->{type} eq "L" && $column->{length} != 1)
      {
         &carp("Columns of type L (logical) must have length 1");
         return undef;
      }
      if ($column->{type} eq "D" && $column->{length} != 8)
      {
         &carp("Columns of type D (date) must have length 8");
         return undef;
      }
   }
   return $pkg_or_self;
}
#----------------

=back

=head1 INSTANCE METHODS

=over 4

=cut

#----------------

=item writeHeader

Write all of the DBF header data to the file.  This truncates the file first.

=cut

sub writeHeader
{
   my $self = shift;

   my $fileHandle = $self->{fh};
   my $header = "";
   $self->{nrecordbytes} = 1; # allow one for the delete byte

   foreach my $column (@{$self->{fields}})
   {
      $self->{nrecordbytes} += $column->{length};
      $header .= pack("a11a1CCCCCCCCCCCCCCCCCCCC",
                      $column->{name}, $column->{type}, (0) x 4,
                      $column->{length}, $column->{decimals}, (0) x 14);
   }
   $header .= pack("C", 0x0D);

   truncate($fileHandle, 0);
   print $fileHandle pack("CCCCVvvCCCCCCCCCCCCCCCCCCCC", $self->{valid}, 
                          $self->{year}%100, $self->{month}, $self->{date}, 
                          $self->{nrecords}, length($header)+32, 
                          $self->{nrecordbytes}, (0)x20);
   print $fileHandle $header;
   return $self;
}
#----------------

=item appendrow_arrayref DATA_ARRAYREF

Add a new row to the end of the DBF file immediately.  The argument
is treated as a reference of fields, in order. The DBF file is altered
as little as possible.

The record count is incremented but is *NOT* written to the file until
the closeDB() method is called (for speed increase).

=cut

sub appendrow_arrayref
{
   my $self = shift;
   my @rows = (shift);

   $self->appendrows_arrayref(\@rows);
}
#----------------

=item appendrows_arrayref ARRAYREF_DATA_ARRAYREFS

Add new rows to the end of the DBF file immediately.  The argument
is treated as a reference of references of fields, in order. The DBF
file is altered as little as possible. The record count is incremented
but is NOT written until the closeDB() method is called (for speed increase).

=cut

sub appendrows_arrayref
{
   my $self = shift;
   my $rows = shift;

   my $FH   = $self->{fh};
   seek($FH,0,2);

   foreach my $row (@$rows)
   {
      if (defined $row)
      {
         $self->{nrecords}++;
         print $FH $self->_packArrayRef($row);
      }
   }

   $self->{rowcache} = undef;  # wipe cache, just in case
}
#----------------

=item appendrow_hashref DATA_HASHREF

Just like appendrow_arrayref, except the incoming data is in a hash.
The DBF columns are used to reorder the data.  Missing values are
converted to blanks.

=cut

sub appendrow_hashref
{
   my $self = shift;
   my @rows = (shift);

   $self->appendrows_hashref(\@rows);
}
#----------------

=item appendrows_hashref ARRAYREF_DATA_HASHREF

Just like appendrows_arrayref, except the incoming data is in a hash.
The DBF columns are used to reorder the data.  Missing values are
converted to blanks.

=cut

sub appendrows_hashref
{
   my $self = shift;
   my $hashrows = shift;

   # Convert hashes to arrays
   my @columnNames = map {$_->{name}} @{$self->{fields}};
   my @arrayrows = ();
   foreach my $row (@$hashrows)
   {
      push @arrayrows, [map {$row->{$_}} @columnNames];
   }

   return $self->appendrows_arrayref(\@arrayrows);
}
#----------------

sub _packArrayRef
{
   my $self = shift;
   my $A_row = shift;
   
   die "Bad row" if (!$A_row);

   my $row = " ";  #start with an undeleted flag
   foreach my $i (0 .. @{$self->{fields}}-1)
   {
      my $column = $self->{fields}->[$i];
      my $v = $A_row->[$i];

      if (defined $v)
      {
         $v = "".$v;
      }
      else
      {
         $v = "";
      }
      my $l = length($v);
      if ($column->{type} eq "N")
      {
         if ($v =~ /\d/)
         {
            $v = sprintf("%$$column{length}.$$column{decimals}f", $v);
         }
         else
         {
            $v = " " x $column->{length};
         }
      }
      elsif ($column->{type} eq "C")
      {
         $v = sprintf("%-$$column{length}s", $v);
      }
      elsif ($column->{type} eq "L")
      {
         $v = ((!$v) || $v =~ /[nNfF]/ ? "F" : "T");
      }
      elsif ($column->{type} eq "D")
      {
         # pass on OK
      }
      else
      {
         die "Unknown type $$column{type}";
      }

      if ($l > $column->{length})
      {
         $v = substr($v, 0, $column->{length});
      }
      $row .= $v;
   }
   return $row;
}
#----------------

=item closeDB

Closes a DBF file after updating the record count.
This is only necessary if you append new rows.

=cut

sub closeDB
{
   my $self = shift;

   $self->writeRecordNumber();
   $self->{fh}->close();
   return $self;
}
#----------------

=item writeRecordNumber

Edits the DBF file to record the current value of nrecords().  This is
useful after appending rows.

=cut

sub writeRecordNumber
{
   my $self = shift;

   my $fileHandle = $self->{fh};
   seek($fileHandle, 4, 0);
   print $fileHandle pack("V",$self->{nrecords});
   return $self;
}
#----------------

sub _readrow
{
   my $self = shift;
   my $row = shift;

   if ($ROWCACHE == 0)
   {
      my $A_rows = $self->_readrows($row,1);
      return $A_rows ? $A_rows->[0] : undef;
   }
   elsif ($self->{rowcache} && $row < $self->{rowcache2} && $row >= $self->{rowcache1})
   {
      return $self->{rowcache}->[$row-$self->{rowcache1}];
   }
   else
   {
      my $num = $ROWCACHE;
      $num = $self->{nrecords} - $row if ($row+$num >= $self->{nrecords});
      $self->{rowcache} = $self->_readrows($row,$num);
      $self->{rowcache1} = $row;
      $self->{rowcache2} = $row+$num;

      return $self->{rowcache}->[0];
   }
}
#----------------

sub _readrows
{
   my $self = shift;
   my $rowStart = shift;
   my $rowCount = shift;

   my @dataRows;

   my $offset = $self->{nheaderbytes} + $rowStart * $self->{nrecordbytes};
   seek($self->{fh},$offset,0);

   my $datarow;
   for (my $r=1; $r<=$rowCount; $r++)
   {
      read($self->{fh}, $datarow, $self->{nrecordbytes});
      my $data = [unpack($self->{packformat}, $datarow)];
      my $delete = shift @$data;
      if ($delete != 32) # 32 is decimal ascii for " "
      {
         push @dataRows, undef;
         next;
      }

      my $nColumns = @$data;
      my $fields = $self->{fields};
      my $type;
      my $col = 0;
      foreach (@$data)
      {
         $type = $fields->[$col++]->{type};
         if ($type eq "C")
         {
            s/ *$//so;
         }
         elsif ($type eq "N")
         {
            s/^ *//so;
         }
         elsif ($type eq "L")
         {
            tr/yYtTnNfF?/111100000/;
         }
      }
      push @dataRows, $data;
   }

   return \@dataRows;
}
#----------------

=item nfields

Return the number of columns in the data table.

=cut

sub nfields
{
   my $self = shift;

   return scalar @{$self->{fields}};
}
#----------------

=item fieldnames

Return a list of field header names.

=cut

sub fieldnames
{
   my $self = shift;

   return (@{$self->{columns}});
}

# Retrieve header metadata for the column spcified by name or number
sub _getfield
{
   my $self = shift;
   my $col = shift;

   if ($col =~ /\D/)
   {
      foreach my $field (@{$self->{fields}})
      {
         return $field if ($field->{name} eq $col);
      }
      return undef;
   }
   else
   {
      return $self->{fields}->[$col];
   }
}
#----------------

=item fieldname COLUMN

Return a the title of the specified column.  COLUMN can be a column
name or number.  Column numbers count from zero.

=cut

sub fieldname
{
   my $self = shift;
   my $col = shift;

   my $field = $self->_getfield($col);
   return undef if (!$field);
   return $field->{name};
}
#----------------

=item fieldtype COLUMN

Return the dBASE field type for the specified column.  COLUMN can be a
column name or number.  Column numbers count from zero.

=cut

sub fieldtype
{
   my $self = shift;
   my $col = shift;

   my $field = $self->_getfield($col);
   return undef if (!$field);
   return $field->{type};
}
#----------------

=item fieldlength COLUMN

Return the byte width for the specified column.  COLUMN can be a
column name or number.  Column numbers count from zero.

=cut

sub fieldlength
{
   my $self = shift;
   my $col = shift;

   my $field = $self->_getfield($col);
   return undef if (!$field);
   return $field->{length};
}
#----------------

=item fielddecimals COLUMN

Return the decimals for the specified column.  COLUMN can be a column
name or number.  Column numbers count from zero.

=cut

sub fielddecimals
{
   my $self = shift;
   my $col = shift;

   my $field = $self->_getfield($col);
   return undef if (!$field);
   return $field->{decimals};
}
#----------------

=item nrecords

Return number of records in the file.

=cut

sub nrecords
{
   my $self = shift;

   return $self->{nrecords};
}
#----------------

=item fetchrow_arrayref ROW

Return a record as a reference to an array of fields.  Row numbers
count from zero.

=cut

sub fetchrow_arrayref
{
   my $self = shift;
   my $row = shift;

   if ($row < 0 || $row >= $self->{nrecords})
   {
      carp("Invalid DBF row: $row");
      return undef;
   }

   return $self->_readrow($row);
}
#----------------

=item fetchrows_arrayref ROW COUNT

Return array ref of records as a reference to an array of fields.
Row numbers start from zero and count is trimed if it excedes table
limits

=cut

sub fetchrows_arrayref
{
   my $self = shift;
   my $rowStart = shift;
   my $rowCount = shift;

   $rowCount = $self->{nrecords}-$rowStart if ($rowStart+$rowCount > $self->{nrecords});

   if ($rowStart < 0 || $rowStart >= $self->{nrecords})
   {
      carp("Invalid DBF row: $rowStart") if $rowStart >= $self->{nrecords};
      return undef;
   }

   return $self->_readrows($rowStart,$rowCount);
}
#----------------

=item fetchrow_hashref ROW

Return a record as a reference to a hash of 
  (field name => field value)
pairs.  Row numbers count from zero.

=cut

sub fetchrow_hashref
{
   my $self = shift;
   my $row = shift;

   my $ref = $self->fetchrow_arrayref($row);
   if (!$ref)
   {
      return undef;
   }
   my $hash = {};
   for my $col (0 .. $#$ref)
   {
      $hash->{$self->{columns}->[$col]} = $ref->[$col];
   }
   return $hash;
}
#----------------

=item fetchrow_array ROW

Return a record as an array of fields.  Row numbers count from zero.

=cut

sub fetchrow_array
{
   my $self = shift;
   my $row = shift;

   my $ref = $self->fetchrow_arrayref($row);
   if (!$ref)
   {
      return ();
   }
   return (@$ref);
}
#----------------

=item delete ROW

Flags a row as deleted.  This alters the DBF file immediately.

=cut

sub delete
{
   my $self = shift;
   my $row = shift;

   return $self->_delete($row, '*');
}
#----------------

=item undelete ROW

Removes the deleted flag from a row.  This alters the DBF file
immediately.

=cut

sub undelete
{
   my $self = shift;
   my $row = shift;

   return $self->_delete($row, ' ');
}

## Internal method only.  Use wrappers above.
sub _delete
{
   my $self = shift;
   my $row = shift;
   my $flag = shift;

   &croak("BAD flag '$flag'") if ($flag ne ' ' && $flag ne '*');

   return undef if (!$row);
   return undef if ($row < 0 || $row >= $self->{nrecords});

   $self->{fh}->close();
   
   my $fh = new FileHandle $self->{filename}, "r+";
   my $result = undef;
   if ($fh)
   {
      my $offset = $self->{nheaderbytes} + $row * $self->{nrecordbytes};
      seek($fh,$offset,0);
      print $fh  $flag;
      $fh->close();
      $result = 1;
   }

   # Reopen main filehandle
   $self->{fh} = new FileHandle $self->{filename}, "r";

   $self->{rowcache} = undef;  # wipe cache, just in case
   return $result ? $self : undef;
}
#----------------

=item toText [STARTROW,] [ENDROW,] [-ARG => VALUE, ...]

Return the contents of the file in an ascii character-separated
representation.  Possible arguments (with default values) are:

    -field      =>  ','
    -enclose    =>  '"'
    -escape     =>  '\'
    -record     =>  '\n'
    -showheader => 0
    -startrow   => 0
    -endrow     => nrecords()-1

Alternatively, if the -arg switches are not used, the first two
arguments are interpreted as:

    toText(startrow,endrow)

Additional -arg switches are permitted after these.  For example:
    print $dbf->toText(100,100,-field=>'\n',-record=>'');
    print $dbf->toText(300,-field=>'|');

=cut

sub toText
{
   my $self = shift;

   my %args = (
               field => ",",
               enclose => "'",
               escape => "\\",
               record => "\n",
               showheader => 0,
               startrow => 0,
               endrow => $self->nrecords()-1,
               );

   foreach my $arg (qw(startrow endrow))
   {
      $args{$arg} = shift if (@_ > 0 && $_[0] !~ /^\-/);
   }

   while (@_ > 0)
   {
      my $key = shift;
      if ($key =~ /^\-(\w+)$/ && exists $args{$1} && @_ > 0)
      {
         $args{$1} = shift;
      }
      else
      {
         carp("Unexpected tag \"$key\" in argument list");
         return undef;
      }
   }

   if ($args{startrow} < 0 || $args{endrow} >= $self->nrecords())
   {
      carp("Invalid start and/or end row");
      return ();
   }
   return () if ($args{startrow} > $args{endrow});

   my $out = "";
   if ($args{showheader}) {
      $out .= join($args{field}, 
                   map({$args{enclose} eq "" && $args{escape} eq "" ? $_ :
                            _escape($_,$args{enclose},$args{escape})}
                       $self->fieldnames())) . $args{record};
   }
   for (my $row = $args{startrow}; $row <= $args{endrow}; $row++)
   {
      my $aref = $self->_readrow($row);
      next if (!$aref);
      if ($args{enclose} ne "" || $args{escape} ne "")
      {
         foreach (@$aref)
         {
            $_ = _escape($_,$args{enclose},$args{escape});
         }
      }
      $out .= join($args{field},@$aref) . $args{record};
   }
   return $out;
}
#----------------

=item computeRecordBytes

Useful primarily for debugging.  Recompute the number of bytes needed
to store a record.

=cut

sub computeRecordBytes
{
   my $self = shift;

   my $length = 1;
   foreach my $column (@{$self->{fields}})
   {
      $length += $column->{length};
   }
   return $length;
}
#----------------

=item computeHeaderBytes

Useful primarily for debugging.  Recompute the number of bytes needed
to store the header.

=cut

sub computeHeaderBytes
{
   my $self = shift;

   my $fh = $self->{fh};
   my $length = 0;
   my ($buffer, $value);
   do
   {
      $length += 32;
      seek $fh, $length, 0;
      read $fh, $buffer, 1;
      $value = unpack("C", $buffer);
   }
   while (defined $buffer && $value != 0x0D && $value != 0x0A);
   return $length + 1; # Add one for the terminator character
}
#----------------

=item computeNumRecords

Useful primarily for debugging.  Recompute the number of records in
the file, given the header size, file size and bytes needed to store a
record.

=cut

sub computeNumRecords
{
   my $self = shift;

   my $size = (-s $self->{filename});
   return int(($size - $self->nHeaderBytes()) / $self->nRecordBytes());
}
#----------------

=item nHeaderBytes

Useful primarily for debugging.  Returns the number of bytes for the
file header.  This date is read from the header itself, not computed.

=cut

sub nHeaderBytes
{
   my $self = shift;
   return $self->{nheaderbytes};
}
#----------------

=item nRecordBytes

Useful primarily for debugging.  Returns the number of bytes for a
record.  This date is read from the header itself, not computed.

=cut

sub nRecordBytes
{
   my $self = shift;
   return $self->{nrecordbytes};
}
#----------------

=item repairHeaderData

Test and fix corruption of the 'nrecords' and 'nrecordbytes' header
fields.  This does NOT alter the file, just the in-memory
representation of the header metadata.  Returns a boolean indicating
whether header repairs were necessary.

=cut

sub repairHeaderData
{
   my $self = shift;

   my $repairs = 0;

   my $rowSize = $self->computeRecordBytes();
   if ($self->nRecordBytes() != $rowSize)
   {
      $repairs++;
      $self->{nrecordbytes} = $rowSize;
   }

   my $nRecords = $self->computeNumRecords();
   if ($nRecords != $self->nrecords())
   {
      $repairs++;
      $self->{nrecords} = $nRecords;
   }

   return $repairs;
}
#----------------

# Internal function
sub _escape
{
   my $string = shift;
   my $enclose = shift;
   my $escape = shift;

   if ($escape ne "")
   {
      $string =~ s/\Q$escape\E/$escape$escape/gs;
      if ($enclose ne "")
      {
         $string =~ s/\Q$enclose\E/$escape$enclose/gs;
      }
   }
   return $enclose . $string . $enclose;
}

1;
__END__

=back

=head1 AUTHOR

Clotho Advanced Media Inc., I<cpan@clotho.com>

Primary developer: Chris Dolan
