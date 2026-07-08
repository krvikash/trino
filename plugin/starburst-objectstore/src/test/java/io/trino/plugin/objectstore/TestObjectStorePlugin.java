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
import com.google.common.collect.Iterators;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestObjectStorePlugin
{
    @Test
    public void testCreateConnectorFailsWithUnusedConfig()
    {
        assertCreateConnectorFails("unused_config", "somevalue", "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails("unused_config", "somevalue", "Configuration property 'unused_config' was not used");
        assertCreateConnectorFails("unused_config", "somevalue", "Configuration property 'unused_config' was not used");
    }

    @Test
    public void testGlueMetastoreWithoutPrefix()
    {
        ConnectorFactory factory = getConnectorFactory();

        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore", "glue")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testUnityMetastore()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore", "unity")
                                .put("hive.metastore.unity.host", "dbc-12345678-abcd")
                                .put("hive.metastore.unity.catalog-name", "main")
                                .put("object-store.iceberg-rest-catalog-used", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testUnityMetastoreWithVendedCredentials()
    {
        ConnectorFactory factory = getConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore", "unity")
                                .put("hive.metastore.unity.host", "dbc-12345678-abcd")
                                .put("hive.metastore.unity.catalog-name", "main")
                                .put("hive.metastore.unity.vended-credentials-enabled", "true")
                                .put("object-store.iceberg-rest-catalog-used", "true")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testCreateConnectorInvalidProperties()
    {
        ConnectorFactory factory = getConnectorFactory();

        // Invalid hive.metastore property
        assertThatThrownBy(() -> factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore", "invalid")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown())
                .hasMessage("Invalid value 'invalid' for 'hive.metastore' configuration property. Supported values are: [THRIFT, FILE, GLUE, UNITY]");

        // Unsupported iceberg.catalog.type property
        assertThatThrownBy(() -> factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore", "thrift")
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("iceberg.catalog.type", "glue")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown())
                .hasMessageContaining("Configuration property 'iceberg.catalog.type' is not supported. Use 'hive.metastore' instead");

        // Unused config property
        assertThatThrownBy(() -> factory.create(
                        "test",
                        ImmutableMap.<String, String>builder()
                                .put("hive.metastore.uri", "thrift://foo:1234")
                                .put("unused_config", "somevalue")
                                .buildOrThrow(),
                        new TestingConnectorContext())
                .shutdown())
                .hasMessageContaining("Configuration property 'unused_config' was not used");
    }

    private static void assertCreateConnectorFails(String key, String value, String exceptionString)
    {
        ConnectorFactory factory = getConnectorFactory();

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put(key, value)
                        .put("hive.metastore.uri", "thrift://localhost:1234")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining(exceptionString);
    }

    private static ConnectorFactory getConnectorFactory()
    {
        return getOnlyElement(Iterators.filter(new ObjectStorePlugin().getConnectorFactories().iterator(), factory -> factory.getName().equals(STARBURST_OBJECTSTORE)));
    }
}
