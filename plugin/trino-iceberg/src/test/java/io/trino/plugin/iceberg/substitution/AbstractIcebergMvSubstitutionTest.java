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
package io.trino.plugin.iceberg.substitution;

import io.trino.plugin.hive.substitution.AbstractMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;

public abstract class AbstractIcebergMvSubstitutionTest
        extends AbstractMvSubstitutionTest
{
    @Override
    protected String partitionedByPropertyName()
    {
        return "partitioning";
    }

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, "tpch");
    }
}
