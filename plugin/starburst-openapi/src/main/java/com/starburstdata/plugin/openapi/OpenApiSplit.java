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
package com.starburstdata.plugin.openapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class OpenApiSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(OpenApiSplit.class);

    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public OpenApiSplit(
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    @JsonProperty("addresses")
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return (long) INSTANCE_SIZE + constraint.getRetainedSizeInBytes(column -> ((OpenApiColumnHandle) column).getRetainedSizeInBytes());
    }
}
