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
package io.trino.plugin.warp.metrics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class WarpStatsBase
{
    @JsonIgnore
    private final String jmxKey;
    private final WarpStatType warpStatType;
    @JsonIgnore
    private Map<String, Long> auditMap = Map.of();

    protected WarpStatsBase(String jmxKey, WarpStatType warpStatType)
    {
        this.jmxKey = jmxKey;
        this.warpStatType = warpStatType;
    }

    @JsonIgnore
    public String getJmxKey()
    {
        return jmxKey;
    }

    public Map<String, LongAdder> getCounters()
    {
        return Map.of();
    }

    public void reset() {}

    public void mergeStats(WarpStatsBase warpStatsBase)
    {
        throw new UnsupportedOperationException();
    }

    public Map<String, Long> statsCounterMapper()
    {
        return Map.of();
    }

    protected Map<String, Long> deltaPrintFields()
    {
        return Map.of();
    }

    protected Map<String, Long> statePrintFields()
    {
        return Map.of();
    }

    public WarpStatType getWarpStatType()
    {
        return warpStatType;
    }

    public Map<String, Object> printStatsMap()
    {
        Map<String, Long> newAuditMap = deltaPrintFields();
        Map<String, Object> res = new HashMap<>();
        newAuditMap.forEach((k, v) -> {
            Long prevVal = (Long) auditMap.get(k);
            if (prevVal == null || !prevVal.equals(v)) {
                res.put(k, formatDeltaValue(v, prevVal));
            }
        });
        res.putAll(statePrintFields());
        auditMap = newAuditMap;
        return res;
    }

    private Map<String, Long> formatDeltaValue(Long newVal, Long prevVal)
    {
        long diff = prevVal == null ? newVal : newVal - prevVal;
        return Map.of("d", diff, "t", newVal);
    }
}
