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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.TransactionBuilder;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.CATALOG;

public final class TestingObjectStoreUtils
{
    private TestingObjectStoreUtils() {}

    public static Map<String, String> createObjectStoreProperties(
            String connectorName,
            TableType tableType,
            Map<String, String> locationSecurityClientConfig,
            String metastoreType,
            Map<String, String> metastoreConfig,
            Map<String, String> hiveS3Config,
            Map<String, String> extraObjectStoreProperties)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(createObjectStoreProperties(tableType));
        properties.putAll(locationSecurityClientConfig);
        properties.putAll(metastoreConfig);
        properties.putAll(hiveS3Config);

        if (!extraObjectStoreProperties.containsKey("hive.metastore-cache-ttl")) {
            // Galaxy uses CachingHiveMetastore by default
            properties.put("hive.metastore-cache-ttl", "2m");
        }
        properties.putAll(extraObjectStoreProperties);

        properties.put("hive.metastore", metastoreType);

        return properties.buildOrThrow();
    }

    private static Map<String, String> createObjectStoreProperties(TableType tableType)
    {
        Map<String, String> properties = new HashMap<>();

        properties.put("object-store.table-type", tableType.toString());
        properties.put("hive.allow-register-partition-procedure", "true");
        properties.put("hive.non-managed-table-writes-enabled", "true");
        properties.put("iceberg.register-table-procedure.enabled", "true");
        properties.put("delta.register-table-procedure.enabled", "true");

        return properties;
    }

    public static <T> T getConnectorService(QueryRunner queryRunner, Class<T> clazz)
    {
        return getConnectorService(queryRunner, CATALOG, clazz);
    }

    public static <T> T getConnectorService(QueryRunner queryRunner, String catalogName, Class<T> clazz)
    {
        return TransactionBuilder.transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl())
                .readOnly()
                .execute(queryRunner.getDefaultSession(), transactionSession -> {
                    return ((ObjectStoreConnector) queryRunner.getCoordinator().getConnector(transactionSession, catalogName)).getInjector().getInstance(clazz);
                });
    }
}
