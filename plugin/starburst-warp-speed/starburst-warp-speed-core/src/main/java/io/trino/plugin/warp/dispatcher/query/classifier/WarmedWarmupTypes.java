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
package io.trino.plugin.warp.dispatcher.query.classifier;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.TransformedColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;

public final class WarmedWarmupTypes
{
    private final ImmutableMap<WarpColumn, WarmUpElement> dataWarmedElements;
    private final ImmutableMap<WarpColumn, WarmUpElement> luceneWarmedElements;
    private final ImmutableListMultimap<WarpColumn, WarmUpElement> basicWarmedElements;
    private final ImmutableMap<WarpColumn, Integer> warpColumnToTotalRecords;
    private final ImmutableSet<WarpColumn> warmedColumns;

    private WarmedWarmupTypes(
            ImmutableMap<WarpColumn, WarmUpElement> dataWarmedElements,
            ImmutableMap<WarpColumn, WarmUpElement> luceneWarmedElements,
            ImmutableListMultimap<WarpColumn, WarmUpElement> basicWarmedElements,
            ImmutableMap<WarpColumn, Integer> warpColumnToTotalRecords,
            ImmutableSet<WarpColumn> warmedColumns)
    {
        this.dataWarmedElements = dataWarmedElements;
        this.luceneWarmedElements = luceneWarmedElements;
        this.basicWarmedElements = basicWarmedElements;
        this.warpColumnToTotalRecords = warpColumnToTotalRecords;
        this.warmedColumns = warmedColumns;
    }

    public ImmutableMap<WarpColumn, WarmUpElement> dataWarmedElements()
    {
        return dataWarmedElements;
    }

    public ImmutableMap<WarpColumn, WarmUpElement> luceneWarmedElements()
    {
        return luceneWarmedElements;
    }

    public ImmutableListMultimap<WarpColumn, WarmUpElement> basicWarmedElements()
    {
        return basicWarmedElements;
    }

    public Optional<WarmUpElement> getByTypeAndColumn(WarmUpType warmUpType, WarpColumn warpColumn, TransformFunction transformFunction)
    {
        return switch (warmUpType) {
            case WARM_UP_TYPE_DATA -> Optional.ofNullable(dataWarmedElements.get(warpColumn));
            case WARM_UP_TYPE_LUCENE -> Optional.ofNullable(luceneWarmedElements.get(warpColumn));
            case WARM_UP_TYPE_BASIC -> basicWarmedElements.get(warpColumn).stream()
                    .filter(x ->
                            ((x.getWarpColumn() instanceof TransformedColumn transformedColumn) &&
                                    Objects.equals(transformedColumn.getTransformFunction(), transformFunction)) ||
                                    (!x.getWarpColumn().isTransformedColumn() &&
                                            Objects.equals(transformFunction, TransformFunction.NONE)))
                    .findFirst();
            case WARM_UP_TYPE_NUM_OF -> throw new RuntimeException();
        };
    }

    public boolean contains(WarpColumn warpColumn, WarmUpType warmUpType, TransformFunction transformFunction)
    {
        return switch (warmUpType) {
            case WARM_UP_TYPE_DATA -> dataWarmedElements.get(warpColumn) != null;
            case WARM_UP_TYPE_LUCENE -> luceneWarmedElements.get(warpColumn) != null;
            case WARM_UP_TYPE_BASIC -> basicWarmedElements.containsKey(warpColumn) &&
                    (((basicWarmedElements.get(warpColumn).stream().findFirst().get().getWarpColumn() instanceof TransformedColumn transformedColumn) &&
                            Objects.equals(transformedColumn.getTransformFunction(), transformFunction)) ||
                            (!basicWarmedElements.get(warpColumn).stream().findFirst().get().getWarpColumn().isTransformedColumn() &&
                                    Objects.equals(transformFunction, TransformFunction.NONE)));
            case WARM_UP_TYPE_NUM_OF -> throw new RuntimeException();
        };
    }

    public ImmutableSet<WarpColumn> getWarmedColumns()
    {
        return warmedColumns;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (WarmedWarmupTypes) obj;
        return Objects.equals(this.dataWarmedElements, that.dataWarmedElements) &&
                Objects.equals(this.luceneWarmedElements, that.luceneWarmedElements) &&
                Objects.equals(this.basicWarmedElements, that.basicWarmedElements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataWarmedElements, luceneWarmedElements, basicWarmedElements);
    }

    @Override
    public String toString()
    {
        return "WarmedWarmupTypes[" +
                "warmedColumns=" + warmedColumns + ", " +
                "warpColumnToTotalRecords=" + warpColumnToTotalRecords + ", " +
                "dataWarmedElements=" + dataWarmedElements + ", " +
                "luceneWarmedElements=" + luceneWarmedElements + ", " +
                "basicWarmedElements=" + basicWarmedElements + ']';
    }

    public boolean isNewColumn(WarpColumn warpColumn)
    {
        return !getWarmedColumns().contains(warpColumn);
    }

    /**
     * get total records from a valid warmup element
     */
    public OptionalInt getColumnTotalRecords(RegularColumn regularColumn)
    {
        Integer totalRecords = warpColumnToTotalRecords.get(regularColumn);
        return totalRecords == null ? OptionalInt.empty() : OptionalInt.of(totalRecords);
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<WarpColumn, WarmUpElement> dataWarmedElements = ImmutableMap.builder();
        private final ImmutableMap.Builder<WarpColumn, WarmUpElement> luceneWarmedElements = ImmutableMap.builder();
        private final ImmutableListMultimap.Builder<WarpColumn, WarmUpElement> basicWarmedElements = ImmutableListMultimap.builder();

        private final Map<WarpColumn, Integer> warpColumnToTotalRecords = new HashMap<>();

        private final ImmutableSet.Builder<WarpColumn> warmedColumns = ImmutableSet.builder();

        public void add(WarmUpElement we)
        {
            switch (we.getWarmUpType()) {
                case WARM_UP_TYPE_DATA -> dataWarmedElements.put(we.getWarpColumn(), we);
                case WARM_UP_TYPE_LUCENE -> luceneWarmedElements.put(we.getWarpColumn(), we);
                case WARM_UP_TYPE_BASIC -> basicWarmedElements.put(we.getWarpColumn(), we);
                case WARM_UP_TYPE_NUM_OF -> throw new RuntimeException();
            }
            warmedColumns.add(we.getWarpColumn());
            if (we.isValid()) {
                Integer previousValue = warpColumnToTotalRecords.putIfAbsent(we.getWarpColumn(), we.getTotalRecords());
                checkArgument(previousValue == null || previousValue == we.getTotalRecords(), "previousValue=%s, weTotalRecords=%s, weWarmUpType=%s, weWarpColumn=%s", previousValue, we.getTotalRecords(), we.getWarmUpType(), we.getWarpColumn());
            }
        }

        public WarmedWarmupTypes build()
        {
            return new WarmedWarmupTypes(
                    dataWarmedElements.buildOrThrow(),
                    luceneWarmedElements.buildOrThrow(),
                    basicWarmedElements.build(),
                    ImmutableMap.copyOf(warpColumnToTotalRecords),
                    warmedColumns.build());
        }
    }
}
