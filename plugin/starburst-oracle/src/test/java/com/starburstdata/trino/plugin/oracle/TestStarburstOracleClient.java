/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.plugin.oracle.OracleSessionProperties;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.DecimalType;
import io.trino.testing.SharedResource.Lease;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.sql.TestTable;
import oracle.jdbc.OracleTypes;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.oracle.OracleQueryRunner.NOOP_LICENSE_MANAGER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstOracleClient
{
    private static final JdbcColumnHandle DECIMAL_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_decimal")
                    .setColumnType(createDecimalType(21, 3))
                    .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), Optional.of(21), Optional.of(3), Optional.empty(), Optional.empty()))
                    .build();

    private static final JdbcColumnHandle DOUBLE_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_double")
                    .setColumnType(DOUBLE)
                    .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.BINARY_DOUBLE, Optional.of("BINARY DOUBLE"), Optional.of(0), Optional.of(0), Optional.empty(), Optional.empty()))
                    .build();

    public static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(ImmutableList.<PropertyMetadata<?>>builder()
                    .addAll(new TypeHandlingJdbcSessionProperties(new TypeHandlingJdbcConfig()).getSessionProperties())
                    .addAll(new OracleSessionProperties(new OracleConfig()).getSessionProperties())
                    .addAll(new StarburstOracleSessionProperties(NOOP_LICENSE_MANAGER, new StarburstOracleConfig()).getSessionProperties())
                    .build())
            .build();

    private final JdbcClient jdbcClient;

    public TestStarburstOracleClient()
    {
        jdbcClient = createJdbcClient();
    }

    // Extension point for SEP. Multiple constructors are disallowed in JUnit.
    public JdbcClient createJdbcClient()
    {
        return new StarburstOracleClient(
                NOOP_LICENSE_MANAGER,
                new BaseJdbcConfig(),
                new JdbcMetadataConfig().setAggregationPushdownEnabled(true),
                new JdbcStatisticsConfig(),
                new OracleConfig(),
                _ -> {
                    throw new UnsupportedOperationException();
                },
                new DefaultQueryBuilder(RemoteQueryModifier.NONE),
                new DefaultIdentifierMapping(),
                RemoteQueryModifier.NONE);
    }

    @Test
    public void testImplementCount()
    {
        Variable decimalVariable = new Variable("v_decimal", createDecimalType(19, 0));
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // count(*)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty()),
                Map.of(),
                Optional.of("count(*)"));

        // count(decimal)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(decimalVariable), List.of(), false, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.of("count(\"c_decimal\")"));

        // count(double)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("count(\"c_double\")"));

        // count(DISTINCT decimal)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(decimalVariable), List.of(), true, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.of("count(DISTINCT \"c_decimal\")"));

        // count() FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, filter),
                Map.of(),
                Optional.empty());

        // count(decimal) FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(decimalVariable), List.of(), false, filter),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.empty());
    }

    @Test
    public void testImplementSum()
    {
        DecimalType decimalType = createDecimalType(21, 3);
        DecimalType decimalSumType = createDecimalType(38, 3);
        Variable decimalVariable = new Variable("v_decimal", decimalType);
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // sum(decimal)
        testImplementAggregation(
                new AggregateFunction("sum", decimalSumType, List.of(decimalVariable), List.of(), false, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.of("sum(\"c_decimal\")"));

        // sum(double)
        testImplementAggregation(
                new AggregateFunction("sum", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("sum(\"c_double\")"));

        // sum(DISTINCT decimal)
        testImplementAggregation(
                new AggregateFunction("sum", decimalSumType, List.of(decimalVariable), List.of(), true, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.of("sum(DISTINCT \"c_decimal\")"));

        // sum(decimal) FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("sum", decimalSumType, List.of(decimalVariable), List.of(), false, filter),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.empty()); // filter not supported
    }

    @Test
    public void testBuildSqlSetsFetchSizeBasedOnColumnCount()
            throws Exception
    {
        try (Lease<TestingStarburstOracleServer> oracleServer = TestingStarburstOracleServer.getInstance();
                Connection connection = DriverManager.getConnection(oracleServer.get().getJdbcUrl(), OracleTestUsers.USER, OracleTestUsers.PASSWORD);
                TestTable table = new TestTable(oracleServer.get().getSqlExecutor(), "test_fetch_size", "(c1 NUMBER, c2 NUMBER, c3 NUMBER, c4 NUMBER)")) {
            List<JdbcColumnHandle> columns = List.of(
                    numberColumn("C1"),
                    numberColumn("C2"),
                    numberColumn("C3"),
                    numberColumn("C4"));

            String schema = connection.getMetaData().getUserName();
            JdbcTableHandle tableHandle = new JdbcTableHandle(
                    new SchemaTableName(OracleTestUsers.USER, table.getName()),
                    new RemoteTableName(Optional.empty(), Optional.of(schema), table.getName().toUpperCase(Locale.ROOT)),
                    Optional.empty());

            OracleSplit split = new OracleSplit(Optional.empty(), Optional.empty(), TupleDomain.all());

            try (PreparedStatement statement = jdbcClient.buildSql(SESSION, connection, split, tableHandle, columns)) {
                assertThat(statement.getFetchSize()).isEqualTo(25_000);
            }
        }
    }

    private static JdbcColumnHandle numberColumn(String name)
    {
        return JdbcColumnHandle.builder()
                .setColumnName(name)
                .setColumnType(BIGINT)
                .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), Optional.of(10), Optional.of(0), Optional.empty(), Optional.empty()))
                .build();
    }

    private void testImplementAggregation(AggregateFunction aggregateFunction, Map<String, ColumnHandle> assignments, Optional<String> expectedExpression)
    {
        Optional<JdbcExpression> result = jdbcClient.implementAggregation(SESSION, aggregateFunction, assignments);
        if (expectedExpression.isEmpty()) {
            assertThat(result).isEmpty();
        }
        else {
            assertThat(result).isPresent();
            assertThat(result.get().getExpression()).isEqualTo(expectedExpression.get());
            Optional<ColumnMapping> columnMapping = jdbcClient.toColumnMapping(SESSION, null, result.get().getJdbcTypeHandle());
            assertThat(columnMapping.isPresent())
                    .as("No mapping for: " + result.get().getJdbcTypeHandle())
                    .isTrue();
            assertThat(columnMapping.get().getType()).isEqualTo(aggregateFunction.getOutputType());
        }
    }
}
