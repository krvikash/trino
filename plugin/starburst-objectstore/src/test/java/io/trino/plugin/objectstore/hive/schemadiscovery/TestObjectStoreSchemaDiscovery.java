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
package io.trino.plugin.objectstore.hive.schemadiscovery;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonMapperProvider;
import io.trino.Session;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hive.containers.HiveFlociDataLake;
import io.trino.plugin.objectstore.ObjectStorePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestObjectStoreSchemaDiscovery
        extends AbstractTestQueryFramework
{
    private static final Set<String> FILES = ImmutableSet.<String>builder()
            .add("schema-discovery/csv/cars.csv")
            .add("schema-discovery/orc/from-trino.orc")
            .add("schema-discovery/parquet/from-trino.parquet")
            .build();
    private static final JsonMapper MAPPER = new JsonMapperProvider().get();
    private static final String BUCKET_NAME = "test-schema-discovery-" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveFlociDataLake hiveFlociDataLake = closeAfterClass(new Hive3FlociDataLake(BUCKET_NAME));
        hiveFlociDataLake.start();
        FILES.forEach(path -> hiveFlociDataLake.floci().copyResources(path, BUCKET_NAME, path));

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("objectstore")
                                .setSchema("schema_discovery")
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("fs.s3.enabled", "true")
                    .put("s3.endpoint", hiveFlociDataLake.floci().endpoint().toString())
                    .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                    .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                    .put("s3.region", FLOCI_REGION)
                    .put("s3.path-style-access", "true")
                    .put("hive.metastore.uri", hiveFlociDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                    .buildOrThrow());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @ParameterizedTest
    @CsvSource({"csv", "orc", "parquet"})
    void testSchemaDiscovery(String directory)
    {
        String uri = BUCKET_NAME + "/schema-discovery/" + directory;
        MaterializedResult discoveryResult = getQueryRunner().execute("SELECT uri, sql, errors FROM discovery WHERE uri = 's3://%s' AND schema = '%s_schema'".formatted(uri, directory));
        assertThat(discoveryResult.getRowCount()).isEqualTo(1);
        MaterializedRow row = discoveryResult.getMaterializedRows().getFirst();
        assertThat(row.getField(0).toString()).isEqualTo("s3://" + uri);
        assertThat(row.getField(2).toString()).isEqualTo("[]");

        String sql = row.getField(1).toString();
        List<String> statements = Splitter.on(";").trimResults().omitEmptyStrings().splitToList(sql);
        // should return three statements: CREATE SCHEMA IF NOT EXISTS <schema>, USE <schema>, CREATE TABLE <table>
        assertThat(statements).hasSize(3);
        String createSchemaStatement = statements.get(0);
        String useSchemaStatement = statements.get(1);
        String createTableStatement = statements.get(2);

        assertThat(createSchemaStatement).startsWith("CREATE SCHEMA IF NOT EXISTS \"%s_schema\"".formatted(directory));
        assertThat(useSchemaStatement).isEqualTo("USE \"%s_schema\"".formatted(directory));
        assertThat(createTableStatement)
                .startsWith("CREATE TABLE \"%s\"".formatted(directory))
                .contains("type = 'hive'");

        assertQuerySucceeds(
                Session.builder(getSession())
                        .setCatalog("objectstore")
                        .build(),
                createSchemaStatement);
        assertQuerySucceeds(
                Session.builder(getSession())
                        .setCatalog("objectstore")
                        .setSchema(directory + "_schema")
                        .build(),
                createTableStatement);
        MaterializedResult tableResult = getQueryRunner().execute("SELECT * FROM %s_schema.%1$s".formatted(directory));
        assertThat(tableResult.getRowCount()).isGreaterThan(0);
    }

    @Test
    void testShallowSchemaDiscovery()
            throws Exception
    {
        String shallowMetadataJson = (String) computeScalar("SELECT shallow_metadata_json FROM shallow_discovery WHERE uri = 's3://" + BUCKET_NAME + "'");
        ArrayNode tables = MAPPER.readTree(shallowMetadataJson)
                .withArray("tables");

        assertThat(tables)
                .hasSize(3)
                .allMatch(table -> table.get("valid").asBoolean())
                .allSatisfy(table -> assertThat((ArrayNode) table.withArray("errors")).hasSize(0))
                .map(table -> entry(table.get("tableName").get("tableName").asText(), table.get("format").asText()))
                .containsOnly(
                        entry("csv", "CSV"),
                        entry("orc", "ORC"),
                        entry("parquet", "PARQUET"));
    }

    @Test
    void testWithMissingWherePredicate()
    {
        assertQueryFails("SELECT * FROM discovery", ".*Missing URI argument.*");
        assertQueryFails("SELECT * FROM discovery WHERE sql IS NOT NULL", ".*Missing URI argument.*");
        assertQueryFails("SELECT * FROM shallow_discovery", ".*Missing URI argument.*");
        assertQueryFails("SELECT * FROM shallow_discovery WHERE shallow_metadata_json IS NOT NULL", ".*Missing URI argument.*");
    }

    @Test
    void testWithIncorrectUri()
    {
        String csvFileName = "schema-discovery/csv/cars.csv";
        String filePath = Resources.getResource(csvFileName).getPath();
        assertQueryFails("SELECT * FROM discovery WHERE uri = 's3://%s/%s'".formatted(BUCKET_NAME, csvFileName), ".*Root directory is empty or isn't a directory.*");
        assertQueryFails("SELECT * FROM discovery WHERE uri = 'abc://%s'".formatted(filePath), ".*No factory for location.*");
        assertQueryFails("SELECT * FROM shallow_discovery WHERE uri = 's3://%s/%s'".formatted(BUCKET_NAME, csvFileName), ".*Root directory is empty or isn't a directory.*");

        // shallow discovery uses a different code path that does not throw TrinoException:
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT * FROM shallow_discovery WHERE uri = 'abc://%s'".formatted(filePath)))
                .hasMessageMatching(".*No factory for location.*");
    }

    @Test
    public void testWithIncorrectUriPredicates()
    {
        String validPathToDir = Resources.getResource("schema-discovery/csv").getPath();
        assertQueryFails("SELECT * FROM discovery WHERE uri < 'file://%s'".formatted(validPathToDir), ".*Only single value is acceptable.*");
        assertQueryFails("SELECT * FROM discovery WHERE uri > 'file://%s'".formatted(validPathToDir), ".*Only single value is acceptable.*");
        assertQueryFails("SELECT * FROM discovery WHERE uri != 'file://%s'".formatted(validPathToDir), ".*Only single value is acceptable.*");
        assertQueryFails("SELECT * FROM shallow_discovery WHERE uri < 'file://%s'".formatted(validPathToDir), ".*Only single value is acceptable.*");
        assertQueryFails("SELECT * FROM shallow_discovery WHERE uri > 'file://%s'".formatted(validPathToDir), ".*Only single value is acceptable.*");
        assertQueryFails("SELECT * FROM shallow_discovery WHERE uri != 'file://%s'".formatted(validPathToDir), ".*Only single value is acceptable.*");
    }
}
