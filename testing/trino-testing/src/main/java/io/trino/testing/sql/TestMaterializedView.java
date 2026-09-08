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
package io.trino.testing.sql;

import io.trino.spi.connector.CatalogSchemaTableName;
import org.intellij.lang.annotations.Language;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class TestMaterializedView
        implements TemporaryRelation
{
    private static final String NO_GRACE_PERIOD = "";

    private final SqlExecutor sqlExecutor;
    private final CatalogSchemaTableName name;

    public TestMaterializedView(SqlExecutor sqlExecutor, CatalogSchemaTableName name, @Language("SQL") String definitionQuery, Map<String, String> properties)
    {
        this(sqlExecutor, name, definitionQuery, properties, Optional.empty());
    }

    public TestMaterializedView(
            SqlExecutor sqlExecutor,
            CatalogSchemaTableName name,
            @Language("SQL") String definitionQuery,
            Map<String, String> properties,
            Optional<Duration> gracePeriod)
    {
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.name = requireNonNull(name, "name is null");
        requireNonNull(definitionQuery, "definitionQuery is null");
        String gracePeriodClause = gracePeriod
                .map(period -> " GRACE PERIOD INTERVAL '%s' SECOND".formatted(requireNonNull(period, "gracePeriod is null").toSeconds()))
                .orElse(NO_GRACE_PERIOD);
        sqlExecutor.execute("CREATE MATERIALIZED VIEW %s%s%s AS %s".formatted(name, gracePeriodClause, propertiesClause(properties), definitionQuery));
    }

    @Override
    public String getName()
    {
        return name.toString();
    }

    public CatalogSchemaTableName mvName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP MATERIALIZED VIEW " + name);
    }

    private static String propertiesClause(Map<String, String> properties)
    {
        if (properties.isEmpty()) {
            return "";
        }
        return properties.entrySet().stream()
                .map(property -> "%s = %s".formatted(property.getKey(), property.getValue()))
                .collect(joining(", ", " WITH (", ")"));
    }
}
