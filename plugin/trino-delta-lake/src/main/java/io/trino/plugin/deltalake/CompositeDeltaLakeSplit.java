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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

public record CompositeDeltaLakeSplit(@JsonProperty("splits") List<DeltaLakeSplit> splits)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(CompositeDeltaLakeSplit.class);

    public CompositeDeltaLakeSplit
    {
        checkArgument(splits.size() > 1, "composite split must contain more than one split");
        splits = ImmutableList.copyOf(splits);
    }

    @Override
    public Optional<String> getAffinityKey()
    {
        // Best-effort: sub-splits may have different affinity keys, and only the first one is used.
        return splits.getFirst().getAffinityKey();
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        double proportion = SplitWeight.rawValueSum(splits, DeltaLakeSplit::splitWeight) / (double) SplitWeight.standard().getRawValue();
        return SplitWeight.fromProportion(proportion);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(splits, DeltaLakeSplit::getRetainedSizeInBytes);
    }
}
