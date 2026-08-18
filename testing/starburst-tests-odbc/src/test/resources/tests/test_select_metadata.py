#
# Copyright Starburst Data, Inc. All rights reserved.
#
# THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
# The copyright notice above does not evidence any
# actual or intended publication of such source code.
#
# Redistribution of this material is strictly prohibited.
#

from math import inf, isnan
from decimal import Decimal


def test_column_name(cursor):
    for column_alias in ["colaa", "xxxax"]:
        for expression in ["1", "'aaa'", "current_time"]:
            cursor.execute("select {0} as {1}".format(expression, column_alias))
            assert _column_description(cursor)[0] == column_alias


_expressions_and_types = [
    # (expression, python type, expected python value)
    ("1", int, 1),
    ("12345", int, 12345),
    ("123456789", int, 123456789),
    ("9223372036854775807", int, 9223372036854775807),
    ("REAL '12345.25'", float, 12345.25),
    ("DOUBLE '12345.00390625'", float, 12345.00390625),  # 0.00390625 == 1/256
    ("DECIMAL '12345.54321'", Decimal, Decimal("12345.54321")),
    ("'1'", str, "1"),
    ("'abc'", str, "abc"),
    ("'Łąka jest piękna.'", str, "Łąka jest piękna."),
    ("CAST('abc' AS char(5))", str, "abc  "),
    ("X'1234567890abcdef'", bytearray, b"\x12\x34\x56\x78\x90\xab\xcd\xef"),
    ("true", bool, True),
    ("false", bool, False),
    ("ARRAY[1, 2]", str, "[1,2]"),
]


def test_column_types(cursor):
    # pyodbc reports the Python class it maps a column to, not the ODBC type code,
    # so char and varchar and their wide variants are indistinguishable here.
    for expression, python_type, _ in _expressions_and_types:
        cursor.execute("select " + expression)
        assert _column_description(cursor)[1] is python_type, (
            "Expression " + expression
        )


def test_varchar_column_size_counts_characters_not_bytes(cursor):
    # 17 characters, 20 bytes in UTF-8.
    cursor.execute("select 'Łąka jest piękna.'")
    assert _column_description(cursor)[4] == 17


def test_decimal_column_size_and_scale(cursor):
    cursor.execute("select DECIMAL '12345.54321'")  # decimal(10,5)
    description = _column_description(cursor)
    assert description[4] == 10
    assert description[5] == 5


def test_expression_values(cursor):
    for expression, _, expected in _expressions_and_types:
        actual = cursor.execute("select " + expression).fetchval()
        message = (
            "Expression {expression} produced {actual}, expected {expected}".format(
                expression=expression, actual=actual, expected=expected
            )
        )
        assert actual == expected, message


def test_expression_values_for_special_doubles(cursor):
    assert cursor.execute("select 1 / 0E0").fetchval() == inf
    assert cursor.execute("select -1 / 0E0").fetchval() == -inf
    assert isnan(cursor.execute("select 0 / 0E0").fetchval())


def _column_description(cursor):
    assert len(cursor.description) == 1
    return cursor.description[0]
