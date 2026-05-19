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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.Session;
import io.trino.annotation.NotThreadSafe;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.operator.BigintGroupByHash.ValuesArray;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.IsolatedClass;

import java.lang.reflect.Constructor;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isAggregationOptimizedGroupByHashEnabled;
import static io.trino.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.type.BigintType.BIGINT;

@NotThreadSafe
public interface GroupByHash
{
    NonEvictableLoadingCache<Type, Class<? extends GroupByHash>> specializedGroupByHashClasses = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .maximumSize(256),
            CacheLoader.from(_ -> isolateGroupByHashClass()));

    static Class<? extends GroupByHash> isolateGroupByHashClass()
    {
        return IsolatedClass.isolateClass(
                new DynamicClassLoader(GroupByHash.class.getClassLoader()),
                GroupByHash.class,
                BigintGroupByHash.class,
                BigintGroupByHash.AddPageWork.class,
                BigintGroupByHash.AddDictionaryPageWork.class,
                BigintGroupByHash.AddRunLengthEncodedPageWork.class,
                BigintGroupByHash.GetGroupIdsWork.class,
                BigintGroupByHash.GetDictionaryGroupIdsWork.class,
                BigintGroupByHash.GetRunLengthEncodedGroupIdsWork.class,
                BigintGroupByHash.DictionaryLookBack.class,
                ValuesArray.class,
                BigintGroupByHash.LongValuesArray.class,
                BigintGroupByHash.IntegerValuesArray.class,
                BigintGroupByHash.ShortValuesArray.class,
                BigintGroupByHash.ByteValuesArray.class);
    }

    static GroupByHash createGroupByHash(
            Session session,
            List<Type> types,
            boolean spillable,
            int expectedSize,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory)
    {
        boolean dictionaryAggregationEnabled = isDictionaryAggregationEnabled(session);
        boolean batchedBigintGroupByHashEnabled = isAggregationOptimizedGroupByHashEnabled(session);
        return createGroupByHash(
                types,
                shouldCacheHashValue(spillable, types),
                expectedSize,
                dictionaryAggregationEnabled,
                batchedBigintGroupByHashEnabled,
                hashStrategyCompiler,
                updateMemory);
    }

    static boolean shouldCacheHashValue(boolean spillable, List<Type> types)
    {
        // Spillable aggregations should always cache hash values since spilling requires sorting by the hash value
        if (spillable) {
            return true;
        }
        // When 3 or more columns are present, always cache the hash value
        if (types.size() >= 3) {
            return true;
        }

        int variableWidthTypes = 0;
        for (Type type : types) {
            // The presence of any container types should trigger hash value caching since computing the hash and
            // checking valueIdentical is so much more expensive for these values
            if (type instanceof MapType || type instanceof ArrayType || type instanceof RowType) {
                return true;
            }
            // Cache hash values when more than 2 or more variable width types are present
            if (type.isFlatVariableWidth()) {
                variableWidthTypes++;
                if (variableWidthTypes >= 2) {
                    return true;
                }
            }
        }
        // All remaining scenarios will use re-compute the hash on-demand during rehashing
        return false;
    }

    static GroupByHash createGroupByHash(
            List<Type> types,
            boolean cacheHashValue,
            int expectedSize,
            boolean dictionaryAggregationEnabled,
            boolean batchedBigintGroupByHashEnabled,
            FlatHashStrategyCompiler hashStrategyCompiler,
            UpdateMemory updateMemory)
    {
        try {
            if (batchedBigintGroupByHashEnabled && types.size() == 1 && BIGINT == types.get(0)) {
                return new SwitchingGroupByHash(new BigintGroupByHashBatched(expectedSize, updateMemory));
            }
            if (types.size() == 1 && BigintGroupByHash.isSupportedType(types.get(0))) {
                Type hashType = getOnlyElement(types);
                Constructor<? extends GroupByHash> constructor = specializedGroupByHashClasses.getUnchecked(hashType).getConstructor(int.class, UpdateMemory.class, Type.class);
                return constructor.newInstance(expectedSize, updateMemory, hashType);
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        return new FlatGroupByHash(
                types,
                cacheHashValue,
                expectedSize,
                dictionaryAggregationEnabled,
                hashStrategyCompiler,
                updateMemory);
    }

    static GroupByHash createBigintGroupByHash(
            UpdateMemory updateMemory,
            int groupCount,
            int nullGroupId,
            long[] valuesByGroupId,
            long[] values,
            int[] groupIds)
    {
        try {
            Constructor<? extends GroupByHash> constructor = specializedGroupByHashClasses.getUnchecked(BIGINT)
                    .getConstructor(UpdateMemory.class, int.class, int.class, long[].class, long[].class, int[].class);
            return constructor.newInstance(updateMemory, groupCount, nullGroupId, valuesByGroupId, values, groupIds);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    long getEstimatedSize();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder);

    /**
     * Signals that no more entries will be inserted, and that only calls to {@link GroupByHash#appendValuesTo(int, PageBuilder)}
     * with sequential groupId values will be observed after this point, allowing the implementation to potentially
     * release memory associated with structures required for inserts or associated with values that have already been
     * output.
     */
    void startReleasingOutput();

    Work<?> addPage(Page page);

    /**
     * The order of new group ids need to be the same as the order of incoming rows,
     * i.e. new group ids should be assigned in rows iteration order
     * Example:
     * rows:      A B C B D A E
     * group ids: 1 2 3 2 4 1 5
     */
    Work<int[]> getGroupIds(Page page);

    long getRawHash(int groupId);

    @VisibleForTesting
    int getCapacity();

    GroupByHash copy();
}
