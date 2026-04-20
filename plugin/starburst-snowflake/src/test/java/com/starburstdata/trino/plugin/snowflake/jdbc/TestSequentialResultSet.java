/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;
import static java.sql.Types.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSequentialResultSet
{
    @Test
    void testEverythingDelegated()
    {
        assertAllMethodsOverridden(ResultSet.class, SequentialResultSet.class);
    }

    @Test
    void testEverythingForwarded()
            throws NoSuchMethodException
    {
        // Methods with custom sequential logic that don't simply delegate to current()
        Set<Method> excludedMethods = ImmutableSet.of(
                ResultSet.class.getMethod("next"),
                ResultSet.class.getMethod("close"),
                ResultSet.class.getMethod("isClosed"));

        assertProperForwardingMethodsAreCalled(
                ResultSet.class,
                delegate -> new SequentialResultSet(delegate, delegate),
                excludedMethods);
    }

    @Test
    void testIteratesBothResultSets()
            throws SQLException
    {
        ResultSet first = createResultSet(new String[] {"a", "b"});
        ResultSet second = createResultSet(new String[] {"c", "d"});

        try (SequentialResultSet sequential = new SequentialResultSet(first, second)) {
            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("a");

            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("b");

            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("c");

            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("d");

            assertThat(sequential.next()).isFalse();
        }
    }

    @Test
    void testEmptyFirstResultSet()
            throws SQLException
    {
        ResultSet first = createResultSet(new String[] {});
        ResultSet second = createResultSet(new String[] {"a", "b"});

        try (SequentialResultSet sequential = new SequentialResultSet(first, second)) {
            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("a");

            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("b");

            assertThat(sequential.next()).isFalse();
        }
    }

    @Test
    void testEmptySecondResultSet()
            throws SQLException
    {
        ResultSet first = createResultSet(new String[] {"a", "b"});
        ResultSet second = createResultSet(new String[] {});

        try (SequentialResultSet sequential = new SequentialResultSet(first, second)) {
            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("a");

            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString("name")).isEqualTo("b");

            assertThat(sequential.next()).isFalse();
        }
    }

    @Test
    void testBothEmpty()
            throws SQLException
    {
        ResultSet first = createResultSet(new String[] {});
        ResultSet second = createResultSet(new String[] {});

        try (SequentialResultSet sequential = new SequentialResultSet(first, second)) {
            assertThat(sequential.next()).isFalse();
        }
    }

    @Test
    void testCloseClosesBoth()
            throws SQLException
    {
        CloseTrackingResultSet first = new CloseTrackingResultSet(createResultSet(new String[] {"a"}));
        CloseTrackingResultSet second = new CloseTrackingResultSet(createResultSet(new String[] {"b"}));

        SequentialResultSet sequential = new SequentialResultSet(first, second);
        assertThat(first.closed).isFalse();
        assertThat(second.closed).isFalse();

        sequential.close();
        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
    }

    @Test
    void testGetMetaDataReturnsFirstMetadata()
            throws SQLException
    {
        ResultSet first = createResultSet(new String[] {"a"});
        ResultSet second = createResultSet(new String[] {"b"});

        try (SequentialResultSet sequential = new SequentialResultSet(first, second)) {
            assertThat(sequential.getMetaData().getColumnCount()).isEqualTo(1);
            assertThat(sequential.getMetaData().getColumnName(1)).isEqualTo("NAME");
        }
    }

    @Test
    void testGetByColumnIndex()
            throws SQLException
    {
        ResultSet first = createResultSet(new String[] {"a"});
        ResultSet second = createResultSet(new String[] {"b"});

        try (SequentialResultSet sequential = new SequentialResultSet(first, second)) {
            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString(1)).isEqualTo("a");

            assertThat(sequential.next()).isTrue();
            assertThat(sequential.getString(1)).isEqualTo("b");
        }
    }

    private static ResultSet createResultSet(String[] values)
            throws SQLException
    {
        CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
        rowSet.setCommand("SELECT 'dummy' AS name");
        // Manually set up metadata and rows
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(1);
        metaData.setColumnName(1, "NAME");
        metaData.setColumnType(1, VARCHAR);
        metaData.setColumnTypeName(1, "VARCHAR");
        rowSet.setMetaData(metaData);

        for (String value : values) {
            rowSet.afterLast();
            rowSet.moveToInsertRow();
            rowSet.updateString(1, value);
            rowSet.insertRow();
            rowSet.moveToCurrentRow();
        }
        rowSet.beforeFirst();
        return rowSet;
    }

    private static class CloseTrackingResultSet
            extends SequentialResultSet
    {
        boolean closed;

        CloseTrackingResultSet(ResultSet delegate)
        {
            super(delegate, createEmptyResultSet());
        }

        @Override
        public void close()
                throws SQLException
        {
            closed = true;
            super.close();
        }

        private static ResultSet createEmptyResultSet()
        {
            try {
                return TestSequentialResultSet.createResultSet(new String[] {});
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
