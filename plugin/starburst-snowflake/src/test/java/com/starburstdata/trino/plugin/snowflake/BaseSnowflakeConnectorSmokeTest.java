/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import com.google.common.base.Suppliers;
import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TemporaryRelation;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseSnowflakeConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private final Supplier<TestDatabase> databaseSupplier = Suppliers.memoize(SnowflakeServer::createTestDatabase);

    protected TestDatabase getTestDatabase()
    {
        return closeAfterClass(databaseSupplier.get());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE -> false;
            case SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_ROW_LEVEL_UPDATE -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    void testMergeWithMixedCasePrimaryKeys()
    {
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_merge_pk_different_cases_" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TABLE " + schema + "." + tableName + " (x int, \"pK\" int NOT NULL, CONSTRAINT pk_" + tableName + " PRIMARY KEY (\"pK\"))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1), (2, 2)", 2);

        assertUpdate("DELETE FROM " + tableName + " WHERE PK = 1", 1);
        assertThat(query("SELECT CAST(x as integer) FROM " + schema + "." + tableName))
                .matches("VALUES 2");

        assertUpdate("UPDATE " + tableName + " SET x = 100 WHERE pk = 2", 1);
        assertThat(query("SELECT CAST(x as integer) FROM " + schema + "." + tableName))
                .matches("VALUES 100");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    protected TemporaryRelation createTestTableForWrites(String tablePrefix)
    {
        TemporaryRelation table = super.createTestTableForWrites(tablePrefix);
        String tableName = table.getName();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;
        onRemoteDatabase().execute(format("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY (%s)", schemaTableName, tableName, "a"));
        return table;
    }

    private SqlExecutor onRemoteDatabase()
    {
        return (sql) -> SnowflakeServer.safeExecuteOnDatabase(getTestDatabase().getName(), sql);
    }
}
