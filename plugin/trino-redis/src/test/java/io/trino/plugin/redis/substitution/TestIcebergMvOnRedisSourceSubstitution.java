/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis.substitution;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.redis.RedisTableDescription;
import io.trino.plugin.redis.util.CodecSupplier;
import io.trino.plugin.redis.util.JsonEncoder;
import io.trino.plugin.redis.util.RedisLoader;
import io.trino.plugin.redis.util.RedisServer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import redis.clients.jedis.RedisClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnRedisSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private RedisServer redisServer;

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        redisServer = closeAfterClass(new RedisServer());
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            JsonCodec<RedisTableDescription> codec = new CodecSupplier<>(RedisTableDescription.class, queryRunner.getPlannerContext().getTypeManager()).get();
            Map<SchemaTableName, RedisTableDescription> tableDescriptions = ImmutableMap.<SchemaTableName, RedisTableDescription>builder()
                    .put(loadDescription(codec, sourceSchema.getSchemaName(), ordersTable.getSchemaTableName().getTableName(), "/substitution/orders.json"))
                    .put(loadDescription(codec, sourceSchema.getSchemaName(), lineitemTable.getSchemaTableName().getTableName(), "/substitution/lineitem.json"))
                    .put(loadDescription(codec, sourceSchema.getSchemaName(), nestedTypeTable.getSchemaTableName().getTableName(), "/substitution/nested.json"))
                    .put(loadDescription(codec, sourceSchema.getSchemaName(), coercionTable.getSchemaTableName().getTableName(), "/substitution/coercion.json"))
                    .buildOrThrow();

            installRedisPlugin(redisServer, queryRunner, tableDescriptions, ImmutableMap.of());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("redis", "source");
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }

    @Override
    protected void createOrdersTable()
    {
        load(ordersTable.getSchemaTableName().getTableName(),
                """
                SELECT orderkey, custkey, CAST(orderdate AS VARCHAR) AS orderdate, totalprice, CAST(orderstatus AS VARCHAR) AS orderstatus
                FROM tpch.tiny.orders WHERE orderkey BETWEEN 20000 AND 20010
                """);
    }

    @Override
    protected void createLineItemTable()
    {
        load(lineitemTable.getSchemaTableName().getTableName(),
                """
                SELECT orderkey, CAST(linenumber AS BIGINT) AS linenumber, quantity, extendedprice, CAST(shipdate AS VARCHAR) AS shipdate
                FROM tpch.tiny.lineitem WHERE orderkey BETWEEN 20000 AND 20010
                """);
    }

    @Override
    protected void createNestedTypeTable(CatalogSchemaTableName table)
    {
        load(table.getSchemaTableName().getTableName(),
                """
                SELECT id, info FROM (VALUES
                    (BIGINT '1', VARCHAR '{"name":"Alice","age":30,"gender":"W"}'),
                    (BIGINT '2', VARCHAR '{"name":"Bob","age":25,"gender":"M"}'),
                    (BIGINT '3', VARCHAR '{"name":"Carol","age":40,"gender":"W"}')) t (id, info)
                """);
    }

    @Override
    protected void createCoercionTable()
    {
        String values = coercionColumns().stream()
                .map(column -> "CAST(%s AS %s)".formatted(column.insertValue(), column.columnType()))
                .collect(Collectors.joining(", "));
        load(coercionTable.getSchemaTableName().getTableName(), "SELECT " + values);
    }

    @Override
    protected SqlExecutor sourceSqlExecutor()
    {
        throw new UnsupportedOperationException("Redis connector is read-only");
    }

    @Override
    protected void dropSourceTable(CatalogSchemaTableName sourceTable)
    {
        // TestingRedisPlugin does not support dropping table, and it is not necessary in this test
    }

    @Test
    @Disabled("Redis is read-only and static-schema: CREATE TABLE AS SELECT and ALTER TABLE ADD COLUMN are not supported on the source")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}

    @Override
    protected void insertIntoOrders(long id)
    {
        RedisClient client = redisServer.getClient();
        String redisKey = ordersTable.getSchemaTableName().getSchemaName() + ":" + ordersTable.getSchemaTableName().getTableName() + ":" + UUID.randomUUID();
        client.set(redisKey, new JsonEncoder().toString(ImmutableMap.of(
                "orderkey", id,
                "custkey", 400,
                "orderdate", "1995-02-01",
                "totalprice", 99.99,
                "orderstatus", "N")));
    }

    private void load(String tableName, String sql)
    {
        TestingTrinoClient trinoClient = getDistributedQueryRunner().getClient();
        try (RedisLoader loader = new RedisLoader(
                trinoClient.getServer(),
                trinoClient.getDefaultSession(),
                redisServer.getClient(),
                sourceSchema().getSchemaName() + ":" + tableName,
                "string")) {
            loader.execute(sql);
        }
    }

    private static Map.Entry<SchemaTableName, RedisTableDescription> loadDescription(JsonCodec<RedisTableDescription> codec, String schema, String tableName, String resource)
            throws IOException
    {
        RedisTableDescription template;
        try (InputStream stream = requireNonNull(
                TestIcebergMvOnRedisSourceSubstitution.class.getResourceAsStream(resource),
                "resource not found: " + resource)) {
            template = codec.fromJson(stream);
        }
        RedisTableDescription description = new RedisTableDescription(tableName, schema, template.key(), template.value());
        return Map.entry(new SchemaTableName(schema, tableName), description);
    }
}
