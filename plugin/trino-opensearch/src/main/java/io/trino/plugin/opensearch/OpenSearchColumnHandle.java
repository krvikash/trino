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
package io.trino.plugin.opensearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record OpenSearchColumnHandle(
        String baseName,
        List<String> dereferenceNames,
        Type type,
        DecoderDescriptor decoderDescriptor,
        boolean supportsPredicates)
        implements ColumnHandle
{
    public OpenSearchColumnHandle
    {
        requireNonNull(baseName, "baseName is null");
        requireNonNull(type, "type is null");
        requireNonNull(decoderDescriptor, "decoderDescriptor is null");
    }

    @JsonIgnore
    public String getQualifiedName()
    {
        return Joiner.on('.')
                .join(ImmutableList.<String>builder()
                        .add(baseName)
                        .addAll(dereferenceNames)
                        .build());
    }

    @Override
    public String toString()
    {
        return baseName() + "::" + type();
    }
}
