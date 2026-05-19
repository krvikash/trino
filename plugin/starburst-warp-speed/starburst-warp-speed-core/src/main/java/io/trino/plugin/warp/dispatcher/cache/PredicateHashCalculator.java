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
package io.trino.plugin.warp.dispatcher.cache;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.spi.block.Block;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

@Singleton
public class PredicateHashCalculator
{
    static final TypeOperators TUPLE_DOMAIN_TYPE_OPERATORS = new TypeOperators();
    private final Map<Type, MethodHandle> hashCodeOperatorsMap;
    private final ShapingLogger shapingLogger;

    @Inject
    public PredicateHashCalculator(ShapingLoggerFactory shapingLoggerFactory)
    {
        shapingLogger = requireNonNull(shapingLoggerFactory).getInstance(PredicateHashCalculator.class);
        hashCodeOperatorsMap = new ConcurrentHashMap<>();
    }

    /**
     * use 64bit hash code, we saw collision in 32bit regular hash
     */
    long getHash(TupleDomain<CacheColumnId> tupleDomain)
            throws Throwable
    {
        if (tupleDomain.getDomains().isEmpty()) {
            return 0;
        }
        Map<CacheColumnId, Long> mapRes = new HashMap<>();
        for (Map.Entry<CacheColumnId, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            if (domain.isAll() || domain.isNone()) {
                mapRes.put(entry.getKey(), (long) entry.hashCode());
            }
            if (domain.getValues() instanceof SortedRangeSet sortedRangeSet) {
                long valuesHash = getSortedRangeSetHash(sortedRangeSet);
                long domainHash = 31 * valuesHash + Boolean.hashCode(domain.isNullAllowed());
                mapRes.put(entry.getKey(), domainHash);
            }
            else {
                mapRes.put(entry.getKey(), (long) domain.hashCode());
            }
        }
        long hashCode = 1;
        for (Map.Entry<CacheColumnId, Long> entry : mapRes.entrySet()) {
            hashCode = 31 * (hashCode * 31 + entry.getKey().hashCode()) + entry.getValue();
        }
        return hashCode;
    }

    private long getSortedRangeSetHash(SortedRangeSet sortedRangeSet)
            throws Throwable
    {
        Type type = sortedRangeSet.getType();
        MethodHandle hashCodeOperator = hashCodeOperatorsMap.computeIfAbsent(type, _ -> TUPLE_DOMAIN_TYPE_OPERATORS.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
        boolean[] inclusive = sortedRangeSet.getInclusive();
        Block sortedRanges = sortedRangeSet.getSortedRanges();
        long hash = Objects.hash(type, Arrays.hashCode(inclusive));
        for (int position = 0; position < sortedRanges.getPositionCount(); position++) {
            boolean positionIsNull = sortedRanges.isNull(position);
            hash = hash * 31 + Boolean.hashCode(positionIsNull);
            if (positionIsNull) {
                continue;
            }
            try {
                hash = hash * 31 + (long) hashCodeOperator.invokeExact(sortedRanges, position);
            }
            catch (Throwable e) {
                shapingLogger.error(e, "failed to calc hash for %s", sortedRanges);
                throw e;
            }
        }
        if (hash == 0) {
            hash = 1;
        }
        return hash;
    }
}
