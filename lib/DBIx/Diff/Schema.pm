package DBIx::Diff::Schema;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use experimental 'smartmatch';
use Log::Any '$log';

use List::Util qw(first);

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
                       diff_db_schema
                       diff_table_schema
               );

our %SPEC;

$SPEC{':package'} = {
    v => 1.1,
    summary => 'Compare schema of two DBI databases',
};

my %common_args = (
    dbh1 => {
        schema => ['obj*'],
        summary => 'DBI database handle for the first table',
        req => 1,
        pos => 0,
    },
    dbh2 => {
        schema => ['obj*'],
        summary => 'DBI database handle for the second table',
        req => 1,
        pos => 1,
    },
);

sub _list_tables {
    my ($dbh) = @_;

    my $driver = $dbh->{Driver}{Name};

    my @res;
    my $sth = $dbh->table_info(undef, undef, undef, undef);
    while (my $row = $sth->fetchrow_hashref) {
        next if $row->{TABLE_TYPE} eq 'VIEW';
        next if $row->{TABLE_SCHEM} =~ /^(information_schema)$/;

        if ($driver eq 'Pg') {
            next if $row->{TABLE_SCHEM} =~ /^(pg_catalog)$/;
        } elsif ($driver eq 'SQLite') {
            next if $row->{TABLE_SCHEM} =~ /^(temp)$/;
            next if $row->{TABLE_NAME} =~ /^(sqlite_master|sqlite_temp_master)$/;
        }

        push @res, join(
            "",
            $row->{TABLE_SCHEM},
            length($row->{TABLE_SCHEM}) ? "." : "",
            $row->{TABLE_NAME},
        );
    }
    sort @res;
}

sub _list_columns {
    my ($dbh, $table) = @_;

    my @res;
    my ($schema, $utable) = split /\./, $table;
    my $sth = $dbh->column_info(undef, $schema, $utable, undef);
    while (my $row = $sth->fetchrow_hashref) {
        push @res, $row;
    }
    sort @res;
}

sub _diff_column_schema {
    my ($c1, $c2) = @_;

    my $res = {};
    {
        if ($c1->{TYPE_NAME} ne $c2->{TYPE_NAME}) {
            $res->{old_type} = $c1->{TYPE_NAME};
            $res->{new_type} = $c2->{TYPE_NAME};
            last;
        }
        if ($c1->{NULLABLE} xor $c2->{NULLABLE}) {
            $res->{old_nullable} = $c1->{NULLABLE};
            $res->{new_nullable} = $c2->{NULLABLE};
        }
        if (defined $c1->{CHAR_OCTET_LENGTH}) {
            if ($c1->{CHAR_OCTET_LENGTH} != $c2->{CHAR_OCTET_LENGTH}) {
                $res->{old_length} = $c1->{CHAR_OCTET_LENGTH};
                $res->{new_length} = $c2->{CHAR_OCTET_LENGTH};
            }
        }
        if (defined $c1->{DECIMAL_DIGITS}) {
            if ($c1->{DECIMAL_DIGITS} != $c2->{DECIMAL_DIGITS}) {
                $res->{old_digits} = $c1->{DECIMAL_DIGITS};
                $res->{new_digits} = $c2->{DECIMAL_DIGITS};
            }
        }
    }
    $res;
}

$SPEC{diff_table_schema} = {
    v => 1.1,
    summary => 'Compare schema of two DBI tables',
    description => <<'_',

This function compares schemas of two DBI tables. You supply two `DBI` database
handles along with table name and this function will return a hash:

    {
        deleted_columns => [...],
        added_columns => [...],
        modified_columns => {
            column1 => {
                old_type => '...',
                new_type => '...',
                ...
            },
        },
    }

_
    args => {
        %common_args,
        table => {
            schema => 'str*',
            name => 'str*',
            summary => 'Table name',
            req => 1,
            pos => 2,
        },
    },
    args_as => "array",
    result_naked => 1,
    "x.perinci.sub.wrapper.disable_validate_args" => 1,
};
sub diff_table_schema {
    my $dbh1    = shift; # VALIDATE_ARG
    my $dbh2    = shift; # VALIDATE_ARG
    my $table   = shift; # VALIDATE_ARG

    #$log->tracef("Comparing table %s ...", $table);

    my @columns1 = _list_columns($dbh1, $table);
    my @columns2 = _list_columns($dbh2, $table);

    #$log->tracef("columns1: %s ...", \@columns1);
    #$log->tracef("columns2: %s ...", \@columns2);

    my (@added, @deleted, %modified);
    for my $c1 (@columns1) {
        my $c1n = $c1->{COLUMN_NAME};
        my $c2 = first {$c1n eq $_->{COLUMN_NAME}} @columns2;
        if (defined $c2) {
            my $tres = _diff_column_schema($c1, $c2);
            $modified{$c1n} = $tres if keys %$tres;
        } else {
            push @deleted, $c1n;
        }
    }
    for my $c2 (@columns2) {
        my $c2n = $c2->{COLUMN_NAME};
        my $c1 = first {$c2n eq $_->{COLUMN_NAME}} @columns1;
        if (defined $c1) {
        } else {
            push @added, $c2n;
        }
    }

    my $res = {};
    $res->{added_columns}    = \@added    if @added;
    $res->{deleted_columns}  = \@deleted  if @deleted;
    $res->{modified_columns} = \%modified if keys %modified;
    $res;
}

$SPEC{diff_db_schema} = {
    v => 1.1,
    summary => 'Compare schemas of two DBI databases',
    description => <<'_',

This function compares schemas of two DBI databases. You supply two `DBI`
database handles and this function will return a hash:

    {
        # list of tables found in first db but missing in second
        deleted_tables => ['table1', ...],

        # list of tables found only in the second db
        added_tables => ['table2', ...],

        # list of modified tables, with details for each
        modified_tables => {
            table3 => {
                deleted_columns => [...],
                added_columns => [...],
                modified_columns => {
                    column1 => {
                        old_type => '...',
                        new_type => '...',
                        ...
                    },
                },
            },
        },
    }

_
    args => {
        %common_args,
    },
    args_as => "array",
    result_naked => 1,
    "x.perinci.sub.wrapper.disable_validate_args" => 1,
};
sub diff_db_schema {
    my $dbh1 = shift; # VALIDATE_ARG
    my $dbh2 = shift; # VALIDATE_ARG

    my @tables1 = _list_tables($dbh1);
    my @tables2 = _list_tables($dbh2);

    $log->tracef("tables1: %s ...", \@tables1);
    $log->tracef("tables2: %s ...", \@tables2);

    my (@added, @deleted, %modified);
    for (@tables1) {
        if ($_ ~~ @tables2) {
            #$log->tracef("Comparing table %s ...", $_);
            my $tres = diff_table_schema($dbh1, $dbh2, $_);
            $modified{$_} = $tres if keys %$tres;
        } else {
            push @deleted, $_;
        }
    }
    for (@tables2) {
        if ($_ ~~ @tables1) {
        } else {
            push @added, $_;
        }
    }

    my $res = {};
    $res->{added_tables}    = \@added    if @added;
    $res->{deleted_tables}  = \@deleted  if @deleted;
    $res->{modified_tables} = \%modified if keys %modified;
    $res;
}

1;
# ABSTRACT:

=head1 SYNOPSIS

 use DBIx::Diff::Schema qw(diff_db_schema diff_table_schema);

 my $res  = diff_db_schema($dbh1, 'dbname1', $dbh2, 'dbname2');

To compare schemas of a single table:

 my $res = diff_table_schema($dbh1, 'dbname1', $dbh2, 'dbname2', 'tablename');


=head1 DESCRIPTION

Currently only tested on Postgres and SQLite.


=head1 SEE ALSO

L<DBIx::Compare> to compare database contents.

=cut
