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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.spi.type.Type;

import static io.trino.plugin.warp.type.TypeUtils.isCharType;
import static io.trino.plugin.warp.type.TypeUtils.isVarcharType;

@Singleton
public class WarmupElementStatsService
{
    private static final int STAT_MAX_SLICE_LENGTH = 8;
    private final ShapingLogger shapingLogger;

    @Inject
    public WarmupElementStatsService(ShapingLoggerFactory shapingLoggerFactory)
    {
        this.shapingLogger = shapingLoggerFactory.getInstance(WarmupElementStatsService.class);
    }

    WarmupElementStats getFinalStats(Type type, WarmupElementStats warmupElementStats, RecTypeCode recTypeCode, WarmUpType warmUpType)
    {
        try {
            if (warmupElementStats.isInitialized()) {
                Object maxValue;
                Object minValue;
                boolean isSingleValue;
                if (recTypeCode.isSupportedFiltering() &&
                        warmUpType != WarmUpType.WARM_UP_TYPE_LUCENE &&
                        (recTypeCode == RecTypeCode.REC_TYPE_VARCHAR ||
                                recTypeCode == RecTypeCode.REC_TYPE_CHAR) &&
                        (isCharType(type) || isVarcharType(type))) {
                    Slice maxSlice = (Slice) warmupElementStats.getMaxValue();
                    Slice minSlice = (Slice) warmupElementStats.getMinValue();
                    isSingleValue = minSlice.length() <= STAT_MAX_SLICE_LENGTH &&
                            maxSlice.length() <= STAT_MAX_SLICE_LENGTH &&
                            warmupElementStats.getNullsCount() == 0 &&
                            minSlice.equals(maxSlice);
                    // if type is Slice we want to save the first 8 bytes for min/max values, for max value we add 1 to last position
                    // need to convert them to byte array in order to preserve the original values
                    byte[] maxSliceValue;
                    if (maxSlice.length() > STAT_MAX_SLICE_LENGTH) {
                        maxSliceValue = maxSlice.getBytes(0, STAT_MAX_SLICE_LENGTH);
                        if (maxSliceValue[STAT_MAX_SLICE_LENGTH - 1] == Byte.MAX_VALUE) {
                            // protect from overflow
                            maxValue = null;
                        }
                        else {
                            // need to increase value by 1 in order to make sure ranges will overlaps (see @RangeMatcher.java)
                            maxSliceValue[STAT_MAX_SLICE_LENGTH - 1]++;
                            if (isCharType(type) && maxSliceValue[STAT_MAX_SLICE_LENGTH - 1] == 32) {
                                // last value in charType can't be a space ' ' [32] value . see CharType::writeSlice
                                maxSliceValue[STAT_MAX_SLICE_LENGTH - 1]++;
                            }
                            maxValue = Slices.wrappedBuffer(maxSliceValue).toStringUtf8();
                        }
                    }
                    else {
                        maxValue = maxSlice.toStringUtf8();
                    }

                    if (minSlice.length() > STAT_MAX_SLICE_LENGTH) {
                        byte[] minSliceValue = minSlice.getBytes(0, STAT_MAX_SLICE_LENGTH);
                        if (isCharType(type) && minSliceValue[minSliceValue.length - 1] == 32) {
                            // last value in charType can't be a space ' ' [32] value . see CharType::writeSlice
                            minSliceValue[minSliceValue.length - 1] = 31;
                        }
                        minValue = Slices.wrappedBuffer(minSliceValue).toStringUtf8();
                    }
                    else {
                        minValue = minSlice.toStringUtf8();
                    }
                }
                else {
                    minValue = warmupElementStats.getMinValue();
                    maxValue = warmupElementStats.getMaxValue();
                    isSingleValue = warmupElementStats.getNullsCount() == 0 && minValue != null && minValue.equals(maxValue);
                }
                warmupElementStats = new WarmupElementStats(warmupElementStats.getNullsCount(), minValue, maxValue, isSingleValue);
            }
            return warmupElementStats;
        }
        catch (Exception e) {
            shapingLogger.warn(e, "failed to get range on write. %s", warmupElementStats);
            return WarmupElementStats.UNINITIALIZED;
        }
    }
}
