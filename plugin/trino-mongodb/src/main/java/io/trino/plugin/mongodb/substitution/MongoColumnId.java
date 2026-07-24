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
package io.trino.plugin.mongodb.substitution;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Column identity for MongoDB MV substitution.
 * <p>
 * MongoDB represents a column as a top-level {@code baseName} plus a {@code dereferenceNames}
 * path into nested document sub-fields. Encoding the full path here keeps the identity of a
 * whole-column scan ({@code dereferenceNames} empty) distinct from a pushed-down sub-field
 * projection, so sub-field access over a substituted MV storage table matches correctly.
 */
public record MongoColumnId(String baseName, List<String> dereferenceNames)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("MongoColumnId", 1);

    public MongoColumnId
    {
        requireNonNull(baseName, "baseName is null");
        dereferenceNames = ImmutableList.copyOf(requireNonNull(dereferenceNames, "dereferenceNames is null"));
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
