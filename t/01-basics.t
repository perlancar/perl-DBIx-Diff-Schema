#!perl

use 5.010;
use strict;
use warnings;

use DBI;
use DBIx::Diff::Struct qw(diff_db_struct);
use File::chdir;
use File::Temp qw(tempdir);
use Test::More 0.98;

my $dir = tempdir(CLEANUP => 1);
$CWD = $dir;
my ($dbh1, $dbh2);

sub connect_db {
    my ($dsn1, $dsn2, $user, $pass);
    if ($dsn1 = $ENV{TEST_DBI_DSN1}) {
        $dsn2 = $ENV{TEST_DBI_DSN2};
        $user = $ENV{TEST_DBI_USER};
        $pass = $ENV{TEST_DBI_PASS};
    } else {
        $dsn1 = "dbi:SQLite:$dir/db1.db";
        $dsn2 = "dbi:SQLite:$dir/db2.db";
        $user = "";
        $pass = "";
    }
    $dbh1 = DBI->connect($dsn1, $user, $pass, {RaiseError=>1});
    $dbh2 = DBI->connect($dsn2, $user, $pass, {RaiseError=>1});
}

sub setup_db {
    $dbh1->do("CREATE TABLE t1 (i INT)");
    $dbh1->do("CREATE TABLE t2 (a INT, i1 INT, f1 FLOAT NOT NULL, d1 DECIMAL(10,3), s1 VARCHAR(10))");

    $dbh2->do("CREATE TABLE t2 (b INT, i1 FLOAT, f1 FLOAT NULL, d1 DECIMAL(12,3), s1 VARCHAR(20))");
    $dbh2->do("CREATE TABLE t3 (i INT)");

    # note: SQLite doesn't give info on decimal digits or varchar length
}

connect_db();
setup_db();

subtest "diff with self" => sub {
    is_deeply(diff_db_struct($dbh1, $dbh1), {});
    is_deeply(diff_db_struct($dbh2, $dbh2), {});
};

subtest "diff" => sub {
    my $res = diff_db_struct($dbh1, $dbh2);
    is_deeply($res, {
        added_tables    => ['main.t3'],
        deleted_tables  => ['main.t1'],
        modified_tables => {
            'main.t2' => {
                added_columns    => ['b'],
                deleted_columns  => ['a'],
                modified_columns => {
                    f1 => {
                        old_nullable => 0,
                        new_nullable => 1,
                    },
                    i1 => {
                        old_type => 'INT',
                        new_type => 'FLOAT',
                    },
                },
            },
        },
    }) or diag explain $res;

    $res = diff_db_struct($dbh2, $dbh1);
    is_deeply($res, {
        added_tables    => ['main.t1'],
        deleted_tables  => ['main.t3'],
        modified_tables => {
            'main.t2' => {
                added_columns    => ['a'],
                deleted_columns  => ['b'],
                modified_columns => {
                    f1 => {
                        old_nullable => 1,
                        new_nullable => 0,
                    },
                    i1 => {
                        old_type => 'FLOAT',
                        new_type => 'INT',
                    },
                },
            },
        },
    }) or diag explain $res;
};

DONE_TESTING:
done_testing();
if (Test::More->builder->is_passing) {
    $CWD = "/";
} else {
    diag "Tests failing, not removing tmpdir $dir";
}
