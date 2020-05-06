package DBIx::Diff::Schema;

# AUTHORITY
# DATE
# DIST
# VERSION

use 5.010001;
use strict 'subs', 'vars';
use warnings;
use Log::ger;

use DBIx::Util::Schema qw(has_table list_tables list_columns);
use List::Util qw(first);

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
                       diff_db_schema
                       diff_table_schema
                       db_schema_eq
                       table_schema_eq
               );

our %SPEC;

$SPEC{':package'} = {
    v => 1.1,
    summary => 'Compare schema of two DBI databases',
};

my %arg0_dbh = (
    dbh => {
        schema => ['obj*'],
        summary => 'DBI database handle',
        req => 1,
        pos => 0,
    },
);

my %arg1_table = (
    table => {
        schema => ['str*'],
        summary => 'Table name',
        req => 1,
        pos => 1,
    },
);

my %diff_db_args = (
    dbh1 => {
        schema => ['obj*'],
        summary => 'DBI database handle for the first database',
        req => 1,
        pos => 0,
    },
    dbh2 => {
        schema => ['obj*'],
        summary => 'DBI database handle for the second database',
        req => 1,
        pos => 1,
    },
);

my %diff_table_args = (
    %diff_db_args,
    table1 => {
        schema => 'str*',
        summary => 'Table name',
        req => 1,
        pos => 2,
    },
    table2 => {
        schema => 'str*',
        summary => 'Second table name (assumed to be the same as first table name if unspecified)',
        pos => 3,
    },
);

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
        if (($c1->{mysql_is_auto_increment} // 0) != ($c2->{mysql_is_auto_increment} // 0)) {
            $res->{old_auto_increment} = $c1->{mysql_is_auto_increment} // 0;
            $res->{new_auto_increment} = $c2->{mysql_is_auto_increment} // 0;
        }
    }
    $res;
}

sub _diff_table_schema {
    my ($dbh1, $dbh2, $table1, $table2) = @_;

    my @columns1 = list_columns($dbh1, $table1);
    my @columns2 = list_columns($dbh2, $table2);

    log_trace("columns1: %s ...", \@columns1);
    log_trace("columns2: %s ...", \@columns2);

    my (@added, @deleted, %modified);
    for my $c1 (@columns1) {
        my $c1n = $c1->{COLUMN_NAME};
        my $c2 = first {$c1n eq $_->{COLUMN_NAME}} @columns2;
        if (defined $c2) {
            my $tres = _diff_column_schema($c1, $c2);
            $modified{$c1n} = $tres if %$tres;
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
    $res->{modified_columns} = \%modified if %modified;
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
        %diff_table_args,
    },
    args_as => "array",
    result_naked => 1,
    "x.perinci.sub.wrapper.disable_validate_args" => 1,
};
sub diff_table_schema {
    my $dbh1    = shift; # VALIDATE_ARG
    my $dbh2    = shift; # VALIDATE_ARG
    my $table1  = shift; # VALIDATE_ARG
    my $table2  = shift // $table1; # VALIDATE_ARG

    #$log->tracef("Comparing table %s vs %s ...", $table1, $table2);

    die "Table $table1 in first database does not exist"
        unless has_table($dbh1, $table1);
    die "Table $table2 in second database does not exist"
        unless has_table($dbh2, $table2);
    _diff_table_schema($dbh1, $dbh2, $table1, $table2);
}

$SPEC{table_schema_eq} = {
    v => 1.1,
    summary => 'Return true if two DBI tables have the same schema',
    description => <<'_',

This is basically just a shortcut for:

    my $res = diff_table_schema(...);
    !%res;

_
    args => {
        %diff_table_args,
    },
    args_as => "array",
    result_naked => 1,
    "x.perinci.sub.wrapper.disable_validate_args" => 1,
};
sub table_schema_eq {
    my $res = diff_table_schema(@_);
    !%$res;
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
        %diff_db_args,
    },
    args_as => "array",
    result_naked => 1,
    "x.perinci.sub.wrapper.disable_validate_args" => 1,
};
sub diff_db_schema {
    my $dbh1 = shift; # VALIDATE_ARG
    my $dbh2 = shift; # VALIDATE_ARG

    my @tables1 = list_tables($dbh1);
    my @tables2 = list_tables($dbh2);

    log_trace("tables1: %s ...", \@tables1);
    log_trace("tables2: %s ...", \@tables2);

    my (@added, @deleted, %modified);
    for my $t (@tables1) {
        if (grep {$_ eq $t} @tables2) {
            #$log->tracef("Comparing table %s ...", $_);
            my $tres = _diff_table_schema($dbh1, $dbh2, $t, $t);
            $modified{$t} = $tres if %$tres;
        } else {
            push @deleted, $t;
        }
    }
    for my $t (@tables2) {
        if (grep {$_ eq $t} @tables1) {
        } else {
            push @added, $t;
        }
    }

    my $res = {};
    $res->{added_tables}    = \@added    if @added;
    $res->{deleted_tables}  = \@deleted  if @deleted;
    $res->{modified_tables} = \%modified if %modified;
    $res;
}

$SPEC{db_schema_eq} = {
    v => 1.1,
    summary => 'Return true if two DBI databases have the same schema',
    description => <<'_',

This is basically just a shortcut for:

    my $res = diff_db_schema(...);
    !%$res;

_
    args => {
        %diff_db_args,
    },
    args_as => "array",
    result_naked => 1,
    "x.perinci.sub.wrapper.disable_validate_args" => 1,
};
sub db_schema_eq {
    my $res = diff_db_schema(@_);
    !%$res;
}

1;
# ABSTRACT:

=head1 SYNOPSIS

 use DBIx::Diff::Schema qw(diff_db_schema diff_table_schema
                           db_schema_eq table_schema_eq);

To compare schemas of whole databases:

 my $res = diff_db_schema($dbh1, $dbh2);
 say "the two dbs are equal" if db_schema_eq($dbh1, $dbh2);

To compare schemas of a single table from two databases:

 my $res = diff_table_schema($dbh1, $dbh2, 'tablename');
 say "the two tables are equal" if table_schema_eq($dbh1, $dbh2, 'tablename');


=head1 DESCRIPTION

Currently only tested on Postgres and SQLite.


=head1 SEE ALSO

L<DBIx::Compare> to compare database contents.

L<diffdb> from L<App::diffdb> which can compare two database (schema as well as
content) and display the result as the familiar colored unified-style diff.

=cut
