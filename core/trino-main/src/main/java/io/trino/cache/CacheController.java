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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.cache.CanonicalSubplan.Key;
import io.trino.cache.CanonicalSubplan.TopNKey;
import io.trino.cache.CanonicalSubplan.TopNRankingKey;

import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.trino.SystemSessionProperties.isSubqueryCacheAggregationsEnabled;
import static io.trino.SystemSessionProperties.isSubqueryCacheProjectionsEnabled;

public class CacheController
{
    /**
     * Logic for cache decision (what to cache, order or caching candidates).
     */
    public List<CacheCandidate> getCachingCandidates(Session session, List<CanonicalSubplan> canonicalSubplans)
    {
        Multimap<SubplanKey, CanonicalSubplan> groupedSubplans = canonicalSubplans.stream()
                .map(subplan -> new SimpleEntry<>(new SubplanKey(subplan), subplan))
                .sorted(Comparator.comparing(entry -> entry.getKey().getPriority()))
                .collect(toImmutableListMultimap(SimpleEntry::getKey, SimpleEntry::getValue));

        List<CacheCandidate> aggregationSubplans = groupedSubplans.entries().stream()
                .filter(entry -> entry.getKey().aggregation())
                .map(entry -> new CacheCandidate(ImmutableList.of(entry.getValue())))
                .collect(toImmutableList());
        List<CacheCandidate> projectionSubplans = groupedSubplans.entries().stream()
                .filter(entry -> !entry.getKey().aggregation())
                .map(entry -> new CacheCandidate(ImmutableList.of(entry.getValue())))
                .collect(toImmutableList());

        ImmutableList.Builder<CacheCandidate> cacheCandidates = ImmutableList.builder();

        if (isSubqueryCacheAggregationsEnabled(session)) {
            cacheCandidates.addAll(aggregationSubplans);
        }

        if (isSubqueryCacheProjectionsEnabled(session)) {
            cacheCandidates.addAll(projectionSubplans);
        }

        return cacheCandidates.build();
    }

    record CacheCandidate(List<CanonicalSubplan> subplans) {}

    record SubplanKey(List<Key> keyChain, boolean aggregation)
    {
        public SubplanKey(CanonicalSubplan subplan)
        {
            this(subplan.getKeyChain(),
                    // TopN and TopNRanking are treated as aggregations because of an assumption of a significant reduction of output rows
                    subplan.getGroupByColumns().isPresent() || subplan.getKey() instanceof TopNKey || subplan.getKey() instanceof TopNRankingKey);
        }

        public int getPriority()
        {
            // prefer deeper plans to be cached first
            return -keyChain.size();
        }
    }
}
