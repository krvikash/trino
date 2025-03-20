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
package io.trino.plugin.warp.storage.write;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.warp.util.SliceUtils;

import java.util.Objects;

public class WarmupElementStats
{
    public static final WarmupElementStats UNINITIALIZED = new WarmupElementStats(false, 0, null, null, false);

    private final boolean initialized;
    private final int nullsCount;
    private final Object maxValue;
    private final Object minValue;
    private final boolean isSingleValue;

    public WarmupElementStats(int nullsCount, Object minValue, Object maxValue)
    {
        this(true, nullsCount, minValue, maxValue, false);
    }

    public WarmupElementStats(int nullsCount, Object minValue, Object maxValue, boolean isSingleValue)
    {
        this(true, nullsCount, minValue, maxValue, isSingleValue);
    }

    @JsonCreator
    public WarmupElementStats(
            @JsonProperty("initialized") boolean initialized,
            @JsonProperty("nullsCount") int nullsCount,
            @JsonProperty("minValue") Object minValue,
            @JsonProperty("maxValue") Object maxValue,
            @JsonProperty("isSingleValue") boolean isSingleValue)
    {
        this.initialized = initialized;
        this.nullsCount = nullsCount;
        // ObjectMapper map String values of min/max to Object type, we need them as Slice objects
        if (maxValue instanceof String string) {
            this.maxValue = SliceUtils.deserializeSlice(string);
        }
        else {
            this.maxValue = maxValue;
        }
        if (minValue instanceof String string) {
            this.minValue = SliceUtils.deserializeSlice(string);
        }
        else {
            this.minValue = minValue;
        }
        this.isSingleValue = isSingleValue;
    }

    @JsonProperty("initialized")
    public boolean isInitialized()
    {
        return initialized;
    }

    @JsonProperty("nullsCount")
    public int getNullsCount()
    {
        return nullsCount;
    }

    @JsonProperty("minValue")
    public Object getMinValue()
    {
        return minValue;
    }

    @JsonProperty("maxValue")
    public Object getMaxValue()
    {
        return maxValue;
    }

    @JsonProperty("isSingleValue")
    public boolean isSingleValue()
    {
        return isSingleValue;
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
        var that = (WarmupElementStats) obj;
        return this.initialized == that.initialized &&
                this.nullsCount == that.nullsCount &&
                this.isSingleValue == that.isSingleValue &&
                Objects.equals(this.maxValue, that.maxValue) &&
                Objects.equals(this.minValue, that.minValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(initialized, nullsCount, maxValue, minValue, isSingleValue);
    }

    @Override
    public String toString()
    {
        return "WarmupElementStats[" +
                "initialized=" + initialized +
                ", nullsCount=" + nullsCount +
                ", maxValue=" + maxValue +
                ", minValue=" + minValue +
                ", isSingleValue=" + isSingleValue +
                ']';
    }
}
