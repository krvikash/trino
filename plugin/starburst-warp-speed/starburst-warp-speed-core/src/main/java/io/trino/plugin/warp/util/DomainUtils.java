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
package io.trino.plugin.warp.util;

import io.airlift.log.Logger;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DomainUtils
{
    private static final Logger logger = Logger.get(DomainUtils.class);

    private DomainUtils()
    {
    }

    public static <T> SimplifyResult<T> simplify(TupleDomain<T> tupleDomain, int predicateThreshold)
    {
        if (tupleDomain.getDomains().isEmpty()) {
            return new SimplifyResult<>(tupleDomain);
        }

        Map<T, Domain> domains = tupleDomain.getDomains().get();
        Map<T, Domain> simplifyDomains = new HashMap<>();
        Set<T> simplifiedColumns = new HashSet<>();
        for (Map.Entry<T, Domain> entry : domains.entrySet()) {
            Domain domain = entry.getValue();
            Domain simplifyDomain = domain;
            if (domain.getValues() instanceof SortedRangeSet sortedRangeSet) {
                if (sortedRangeSet.getRangeCount() > predicateThreshold) {
                    logger.debug("Simplifying the domain of column %s. rangeCount=%d > predicateThreshold=%d",
                            entry.getKey(), sortedRangeSet.getRangeCount(), predicateThreshold);
                    simplifyDomain = domain.simplify(predicateThreshold);
                    simplifiedColumns.add(entry.getKey());
                }
            }
            else {
                simplifyDomain = domain.simplify(predicateThreshold);
                if (!simplifyDomain.equals(domain)) {
                    simplifiedColumns.add(entry.getKey());
                }
            }
            simplifyDomains.put(entry.getKey(), simplifyDomain);
        }
        return new SimplifyResult<>(TupleDomain.withColumnDomains(simplifyDomains), simplifiedColumns);
    }
}
