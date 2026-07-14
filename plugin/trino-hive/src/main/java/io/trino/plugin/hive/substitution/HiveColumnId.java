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
package io.trino.plugin.hive.substitution;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorIdVersion;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Identity of a Hive column reference. {@code dereferenceNames} is the struct-field path of a
 * projected sub-field (empty for a whole base column); it keeps two sub-fields of the same base
 * column distinct even when they share a type, e.g. {@code info.a} vs {@code info.b}.
 */
public record HiveColumnId(String baseColumnName, List<String> dereferenceNames)
        implements ConnectorColumnId
{
    public static final ConnectorIdVersion VERSION = new ConnectorIdVersion("HiveColumnId", 1);

    public HiveColumnId
    {
        requireNonNull(baseColumnName, "baseColumnName is null");
        dereferenceNames = ImmutableList.copyOf(dereferenceNames);
    }

    @Override
    public ConnectorIdVersion version()
    {
        return VERSION;
    }
}
