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
package io.trino.operator.aggregation.partial;

import io.airlift.units.DataSize;
import io.trino.operator.HashAggregationOperator;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Controls whenever partial aggregation is enabled across all {@link HashAggregationOperator}s
 * for a particular plan node on a single node.
 * Partial aggregation is disabled after sampling sufficient amount of input
 * and the ratio between output(unique) and input rows is too high (> {@link #uniqueRowsRatioThreshold}).
 * <p>
 * The class is thread safe and objects of this class are used potentially by multiple threads/drivers simultaneously.
 * Different threads either:
 * - modify fields via synchronized {@link #onFlush}.
 * - read volatile {@link #aggregationMode} (volatile here gives visibility).
 */
public class PartialAggregationController
{
    /**
     * Process enough pages to fill up partial-aggregation buffer before
     * considering partial-aggregation to be turned off.
     */
    private static final double DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_FACTOR = 1.5;
    /**
     * Re-enable partial aggregation periodically in case aggregation efficiency improved.
     */
    private static final double ENABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_FACTOR = DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_FACTOR * 200;

    private static final double SIGMOID_INPUT_VALUE_FOR_HALF_OUTPUT_BYTES_FACTOR = 0.125;

    private static final double SLOPE_FOR_SIGMOID_FUNCTION = 0.00001;

    private final boolean useCardinalityBasedController;
    private final DataSize maxPartialMemory;
    private final double uniqueRowsRatioThreshold;

    private volatile AggregationMode aggregationMode = AggregationMode.HLL;
    private long totalBytesProcessed;
    private long totalRowProcessed;
    private long totalUniqueRowsProduced;

    public PartialAggregationController(boolean useCardinalityBasedController, DataSize maxPartialMemory, double uniqueRowsRatioThreshold)
    {
        this.useCardinalityBasedController = useCardinalityBasedController;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        this.uniqueRowsRatioThreshold = uniqueRowsRatioThreshold;
        this.aggregationMode = useCardinalityBasedController ? AggregationMode.HLL : AggregationMode.AGGREGATION;
    }

    public AggregationMode getPartialAggregationMode()
    {
        return aggregationMode;
    }

    public synchronized void onFlush(long bytesProcessed, long rowsProcessed, OptionalLong uniqueRowsProduced)
    {
        if ((aggregationMode == AggregationMode.AGGREGATION) && uniqueRowsProduced.isEmpty()) {
            // when PA is re-enabled, ignore stats from disabled flushes
            return;
        }

        totalBytesProcessed += bytesProcessed;
        totalRowProcessed += rowsProcessed;
        uniqueRowsProduced.ifPresent(value -> totalUniqueRowsProduced += value);

        if (aggregationMode == AggregationMode.HLL) {
            // Use a sigmoid based function to identify if we need to process in PA mode - to determine low cardinlaity stream early,
            // else after processing maxPartialMemory.toBytes() * DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_FACTOR switch to PASSTHROUGH_MODE
            if (shouldUsePartialAggregationMode(totalUniqueRowsProduced, totalRowProcessed, totalBytesProcessed, uniqueRowsRatioThreshold, SLOPE_FOR_SIGMOID_FUNCTION, maxPartialMemory.toBytes() * SIGMOID_INPUT_VALUE_FOR_HALF_OUTPUT_BYTES_FACTOR)) {
                aggregationMode = AggregationMode.AGGREGATION;
            }
            else {
                if (totalBytesProcessed >= maxPartialMemory.toBytes() * DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_FACTOR) {
                    aggregationMode = AggregationMode.PASSTHROUGH;
                }
            }
        }

        if (aggregationMode == AggregationMode.AGGREGATION && shouldDisablePartialAggregation()) {
            aggregationMode = AggregationMode.PASSTHROUGH;
        }

        if (aggregationMode == AggregationMode.PASSTHROUGH
                && totalBytesProcessed >= maxPartialMemory.toBytes() * ENABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_FACTOR) {
            totalBytesProcessed = 0;
            totalRowProcessed = 0;
            totalUniqueRowsProduced = 0;
            aggregationMode = useCardinalityBasedController ? AggregationMode.HLL : AggregationMode.AGGREGATION;
        }
    }

    private boolean shouldDisablePartialAggregation()
    {
        return totalBytesProcessed >= maxPartialMemory.toBytes() * DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_FACTOR
                && ((double) totalUniqueRowsProduced / totalRowProcessed) > uniqueRowsRatioThreshold;
    }

    public PartialAggregationController duplicate()
    {
        return new PartialAggregationController(useCardinalityBasedController, maxPartialMemory, uniqueRowsRatioThreshold);
    }

    public enum AggregationMode
    {
        AGGREGATION,
        HLL,
        PASSTHROUGH
    }

    /**
     * Uses a sigmoid specific function very the threshold and identify if the stream can be processed better by AGGREGATION mode.
     * Signmoid function allows us to take a decision on smaller ratio of unique values over a smaller set of data and also avoids
     * noise in the initial stream of data.
     */
    public static boolean shouldUsePartialAggregationMode(long totalUniqueRowsProduced, long totalRowProcessed, long totalBytesProcessed, double baseThreshold, double slope, double midpointThreshold)
    {
        double weight = sigmoid(totalBytesProcessed, slope, midpointThreshold);
        double dynamicThreshold = baseThreshold * weight;
        return (double) totalUniqueRowsProduced / totalRowProcessed < dynamicThreshold;
    }

    /**
     * Simple sigmoid function
     */
    public static double sigmoid(double input, double slope, double midpoint)
    {
        return 1.0 / (1.0 + Math.exp(-slope * (input - midpoint)));
    }
}
