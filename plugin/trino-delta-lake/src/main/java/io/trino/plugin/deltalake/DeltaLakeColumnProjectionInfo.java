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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
import static java.util.Objects.requireNonNull;

public class DeltaLakeColumnProjectionInfo
{
    private static final int INSTANCE_SIZE = instanceSize(DeltaLakeColumnProjectionInfo.class);

    private final Type type;
    private final List<Integer> dereferenceIndices;
    private final List<String> dereferenceNames;

    @JsonCreator
    public DeltaLakeColumnProjectionInfo(
            @JsonProperty("type") Type type,
            @JsonProperty("dereferenceIndices") List<Integer> dereferenceIndices,
            @JsonProperty("dereferenceNames") List<String> dereferenceNames)
    {
        this.type = requireNonNull(type, "type is null");
        requireNonNull(dereferenceIndices, "dereferenceIndices is null");
        requireNonNull(dereferenceNames, "dereferenceNames is null");
        checkArgument(dereferenceIndices.size() > 0, "dereferenceIndices should not be empty");
        checkArgument(dereferenceNames.size() > 0, "dereferenceNames should not be empty");
        checkArgument(dereferenceIndices.size() == dereferenceNames.size(), "dereferenceIndices and dereferenceNames should have the same sizes");
        this.dereferenceIndices = ImmutableList.copyOf(dereferenceIndices);
        this.dereferenceNames = ImmutableList.copyOf(dereferenceNames);
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public List<Integer> getDereferenceIndices()
    {
        return dereferenceIndices;
    }

    @JsonProperty
    public List<String> getDereferenceNames()
    {
        return dereferenceNames;
    }

    @JsonIgnore
    public String getPartialName()
    {
        return String.join("#", dereferenceNames);
    }

    @JsonIgnore
    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + estimatedSizeOf(dereferenceIndices, SizeOf::sizeOf)
                + estimatedSizeOf(dereferenceNames, SizeOf::estimatedSizeOf);
    }

    public HiveColumnProjectionInfo toHiveColumnProjectionInfo()
    {
        return new HiveColumnProjectionInfo(dereferenceIndices, dereferenceNames, toHiveType(type), type);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaLakeColumnProjectionInfo that = (DeltaLakeColumnProjectionInfo) o;
        return Objects.equals(this.type, that.type)
                && Objects.equals(this.dereferenceIndices, that.dereferenceIndices)
                && Objects.equals(this.dereferenceNames, that.dereferenceNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, dereferenceIndices, dereferenceNames);
    }

    @Override
    public String toString()
    {
        return getPartialName() + ":" + type.getDisplayName();
    }
}
