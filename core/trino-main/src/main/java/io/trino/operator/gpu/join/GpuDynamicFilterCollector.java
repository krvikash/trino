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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.GroupByAggregation;
import ai.rapids.cudf.GroupByOptions;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.gpu.GpuTypeConversion.FromHostValue;
import io.trino.spi.gpu.GpuTypeConversion.FromScalar;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.planner.DynamicFilterDomain;
import io.trino.sql.planner.DynamicFilterSourceConsumer;
import io.trino.sql.planner.DynamicFilterTupleDomain;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.TypeUtils.isFloatingPointNaN;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static java.util.Objects.requireNonNull;

public final class GpuDynamicFilterCollector
{
    public record Channel(DynamicFilterId filterId, int channelIndex, Type type, Optional<FromHostValue> fromHostValue, Optional<FromScalar> fromScalar)
    {
        public Channel
        {
            requireNonNull(filterId, "filterId is null");
            requireNonNull(type, "type is null");
            requireNonNull(fromHostValue, "fromHostValue is null");
            requireNonNull(fromScalar, "fromScalar is null");
        }
    }

    private final DynamicFilterSourceConsumer consumer;
    private final List<Channel> channels;
    private final int maxDistinctValues;

    public GpuDynamicFilterCollector(DynamicFilterSourceConsumer consumer, List<Channel> channels, int maxDistinctValues)
    {
        this.consumer = requireNonNull(consumer, "consumer is null");
        this.channels = ImmutableList.copyOf(requireNonNull(channels, "channels is null"));
        this.maxDistinctValues = maxDistinctValues;
    }

    public void collectEmpty()
    {
        if (consumer.isDomainCollectionComplete()) {
            return;
        }
        consumer.addPartition(DynamicFilterTupleDomain.none());
    }

    public void collect(@Borrow Table buildTable)
    {
        requireNonNull(buildTable, "buildTable is null");
        if (consumer.isDomainCollectionComplete()) {
            return;
        }

        ImmutableMap.Builder<DynamicFilterId, DynamicFilterDomain> domains = ImmutableMap.builder();
        for (Channel channel : channels) {
            @Borrow ColumnVector column = buildTable.getColumn(channel.channelIndex());
            domains.put(channel.filterId(), collectDomain(column, channel));
        }
        consumer.addPartition(DynamicFilterTupleDomain.withColumnDomains(domains.buildOrThrow()));
    }

    private DynamicFilterDomain collectDomain(@Borrow ColumnVector column, Channel channel)
    {
        Type type = channel.type();

        if (channel.fromHostValue().isEmpty() && channel.fromScalar().isEmpty()) {
            return DynamicFilterDomain.all(type);
        }

        try (Table table = new Table(column);
                Table distinctTable = table
                        .groupBy(GroupByOptions.builder().withIgnoreNullKeys(true).build(), 0)
                        .aggregate(GroupByAggregation.nth(0).onColumn(0))) {
            long distinctCount = distinctTable.getRowCount();
            if (distinctCount == 0) {
                return DynamicFilterDomain.none(type);
            }
            if (distinctCount <= maxDistinctValues && channel.fromHostValue().isPresent()) {
                List<Object> values = extractDistinctValues(type, distinctTable.getColumn(0), channel.fromHostValue().get());
                return DynamicFilterDomain.fromDomain(Domain.create(ValueSet.copyOf(type, values), false));
            }
        }

        if (type.isOrderable() && !typeHasNaN(type) && channel.fromScalar().isPresent()) {
            FromScalar fromScalar = channel.fromScalar().get();
            try (Scalar minScalar = column.min(); Scalar maxScalar = column.max()) {
                // cuDF reduce() returns invalid scalar on reduction failure; fall through to all() as a safe fallback
                if (minScalar.isValid() && maxScalar.isValid()) {
                    Object min = fromScalar.trinoValue(minScalar);
                    Object max = fromScalar.trinoValue(maxScalar);
                    return DynamicFilterDomain.fromDomain(Domain.create(ValueSet.ofRanges(Range.range(type, min, true, max, true)), false));
                }
            }
        }

        return DynamicFilterDomain.all(type);
    }

    private static List<Object> extractDistinctValues(Type type, @Borrow ColumnVector distinctColumn, FromHostValue fromHostValue)
    {
        // TODO consider using CopyToBlocks.createColumnCopier to build a Block and readNativeValue to avoid megamorphic dispatch on fromHostValue
        try (HostColumnVector columnVector = distinctColumn.copyToHost()) {
            ImmutableList.Builder<Object> values = ImmutableList.builder();
            for (int i = 0; i < columnVector.getRowCount(); i++) {
                if (columnVector.isNull(i)) {
                    continue;
                }
                Object value = fromHostValue.trinoValue(columnVector, i);
                if (!isFloatingPointNaN(type, value)) {
                    values.add(value);
                }
            }
            return values.build();
        }
    }
}
