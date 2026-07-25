
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
package io.trino.plugin.warp.gen.stats;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.warp.metrics.WarpStatsBase;
import org.weakref.jmx.Managed;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName", "unused"})
public final class ConnectorStats
        extends WarpStatsBase
{
    /* This class file is auto-generated from connector xml file for statistics and counters */
    private final LongBuffer longStruct;

    @JsonCreator
    public ConnectorStats(@JsonProperty("connector_stats_register") long connector_stats_register, @JsonProperty("connector_stats_reject") long connector_stats_reject, @JsonProperty("connector_stats_loader_in_use") long connector_stats_loader_in_use)
    {
        this();
        longStruct.put(0, connector_stats_register);
        longStruct.put(1, connector_stats_reject);
        longStruct.put(2, connector_stats_loader_in_use);
    }

    public ConnectorStats()
    {
        super("connector");

        ByteBuffer rawStruct = initNative(1);
        rawStruct.order(ByteOrder.LITTLE_ENDIAN);
        longStruct = rawStruct.asLongBuffer();
    }

    @JsonIgnore
    @JsonProperty("connector_stats_register")
    @Managed
    public long getconnector_stats_register()
    {
        return longStruct.get(0);
    }

    @JsonIgnore
    @JsonProperty("connector_stats_reject")
    @Managed
    public long getconnector_stats_reject()
    {
        return longStruct.get(1);
    }

    @JsonIgnore
    @JsonProperty("connector_stats_loader_in_use")
    @Managed
    public long getconnector_stats_loader_in_use()
    {
        return longStruct.get(2);
    }

    @Override
    public void reset()
    {
        longStruct.put(0, 0);
        longStruct.put(1, 0);
        longStruct.put(2, 0);
    }

    @Override
    protected Map<String, Long> deltaPrintFields()
    {
        Map<String, Long> res = new HashMap<>();
        res.put("connector_stats_loader_in_use", getconnector_stats_loader_in_use());
        return res;
    }

    @Override
    protected Map<String, Long> statePrintFields()
    {
        return new HashMap<>();
    }

    private native ByteBuffer initNative(long limit);
}
