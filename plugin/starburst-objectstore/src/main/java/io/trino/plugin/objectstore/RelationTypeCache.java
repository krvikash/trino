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
package io.trino.plugin.objectstore;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.stats.DecayCounter;
import io.airlift.stats.ExponentialDecay;
import io.trino.spi.connector.SchemaTableName;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.HIVE;
import static io.trino.plugin.objectstore.TableType.HUDI;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.function.Function.identity;

@ThreadSafe
public final class RelationTypeCache
{
    private static final TableType DEFAULT_TABLE_TYPE = ICEBERG;
    private static final List<TableType> DEFAULT_TABLE_TYPE_ORDER = ImmutableList.of(ICEBERG, DELTA, HUDI, HIVE);
    private static final EnumMap<TableType, List<TableType>> ORDER_BY_CACHED_TYPE;

    static {
        ORDER_BY_CACHED_TYPE = new EnumMap<>(TableType.class);
        for (TableType firstType : DEFAULT_TABLE_TYPE_ORDER) {
            ImmutableList.Builder<TableType> order = ImmutableList.builderWithExpectedSize(DEFAULT_TABLE_TYPE_ORDER.size());
            order.add(firstType);
            for (TableType type : DEFAULT_TABLE_TYPE_ORDER) {
                if (type != firstType) {
                    order.add(type);
                }
            }
            ORDER_BY_CACHED_TYPE.put(firstType, order.build());
        }
    }

    private final Cache<SchemaTableName, RelationType> cache;
    private final Map<TableType, DecayCounter> tableTypeCounters;

    public RelationTypeCache()
    {
        // it is acceptable for this cache to sometimes return stale value,
        // it is only used to decide about order of calls, so it doesn't affect correctness
        // for the same reason there is no invalidation logic or any other similar mechanism enabled
        this.cache = buildUnsafeCacheWithInvalidationRace(CacheBuilder.newBuilder()
                .expireAfterWrite(1, HOURS)
                .maximumSize(10_000));

        tableTypeCounters = DEFAULT_TABLE_TYPE_ORDER.stream()
                .collect(toImmutableMap(identity(), ignore -> new DecayCounter(ExponentialDecay.seconds(toIntExact(HOURS.toSeconds(1))))));
        tableTypeCounters.get(DEFAULT_TABLE_TYPE).add(1);
    }

    @SuppressModernizer
    private static <K, V> Cache<K, V> buildUnsafeCacheWithInvalidationRace(CacheBuilder<? super K, ? super V> cacheBuilder)
    {
        return cacheBuilder.build();
    }

    public Optional<RelationType> getRelationType(SchemaTableName tableName)
    {
        return Optional.ofNullable(cache.getIfPresent(tableName));
    }

    public List<TableType> getTableTypeAffinity(SchemaTableName tableName)
    {
        TableType tableType = getRelationType(tableName)
                .flatMap(RelationType::tableType)
                .orElse(null);
        if (tableType == null) {
            tableType = tableTypeCounters.entrySet().stream()
                    .max(comparing(entry -> entry.getValue().getRate()))
                    .orElseThrow().getKey();
        }
        return ORDER_BY_CACHED_TYPE.get(tableType);
    }

    public void record(SchemaTableName tableName, TableType currentTableType)
    {
        record(tableName, RelationType.from(currentTableType));
    }

    public void record(SchemaTableName tableName, RelationType relationType)
    {
        // it is acceptable for this cache to have a stale value
        cache.put(tableName, relationType);
        relationType.tableType().ifPresent(tableType -> tableTypeCounters.get(tableType).add(1));
    }
}
