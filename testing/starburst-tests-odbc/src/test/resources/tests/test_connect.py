#
# Copyright Starburst Data, Inc. All rights reserved.
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#
# Redistribution of this material is strictly prohibited.
#

import pytest
import pyodbc
from contextlib import closing
from . import TestingStarburst
from . import format_connection_string, pyodbc_connect


def test_initiate_tests(testing_starburst):
    assert testing_starburst is not None


def test_connect(configuration, testing_starburst):
    connection_string = format_connection_string(configuration, testing_starburst)
    connection = pyodbc.connect(connection_string, timeout=10, autocommit=True)
    try:
        assert connection.cursor().execute("select 42").fetchval() == 42
    finally:
        connection.close()


def test_smoke_test(cursor):
    assert cursor.execute("select count(*) from tpch.tiny.nation").fetchval() == 25


def test_connect_autocommit_off_not_supported(configuration, testing_starburst):
    # ODBC v3 always operates in autocommit mode; explicitly disabling it is rejected.
    connection_string = format_connection_string(configuration, testing_starburst)

    with pytest.raises(pyodbc.Error) as exc_info:
        pyodbc.connect(connection_string, timeout=10, autocommit=False)
    assert "HY024" in str(exc_info.value)


def test_connect_no_schema(configuration, testing_starburst):
    connection_string = format_connection_string(configuration, testing_starburst)

    with pyodbc_connect(connection_string, timeout=10) as connection:
        with pytest.raises(pyodbc.Error) as exc_info:
            connection.cursor().execute("select count(*) from nation")
        assert "Schema must be specified when session schema is not set" in str(
            exc_info.value
        )

    with pyodbc_connect(connection_string, timeout=10) as connection:
        assert (
            connection.cursor()
            .execute("select count(*) from tpch.tiny.nation")
            .fetchval()
            == 25
        )

def test_connect_to_schema(configuration, testing_starburst):
    connection_string = format_connection_string(configuration, testing_starburst)
    connection_string += ";catalog=tpch;schema=tiny"

    with pyodbc_connect(connection_string, timeout=10) as connection, closing(
        connection.cursor()
    ) as cursor:
        assert cursor.execute("select count(*) from nation").fetchval() == 25
