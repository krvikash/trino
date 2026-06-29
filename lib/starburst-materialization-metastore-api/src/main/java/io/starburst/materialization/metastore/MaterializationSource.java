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
package io.starburst.materialization.metastore;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.spi.connector.CatalogSchemaTableName;

import static java.util.Objects.requireNonNull;

/**
 * Identifies the source of materialization — currently only materialized views.
 * The main purpose is to help manage the materialization lifecycle.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes(@JsonSubTypes.Type(value = MaterializationSource.MaterializedViewSource.class, name = "materializedView"))
public sealed interface MaterializationSource
        permits MaterializationSource.MaterializedViewSource
{
    record MaterializedViewSource(CatalogSchemaTableName materializedViewName)
            implements MaterializationSource
    {
        public MaterializedViewSource
        {
            requireNonNull(materializedViewName, "materializedViewName is null");
        }
    }
}
