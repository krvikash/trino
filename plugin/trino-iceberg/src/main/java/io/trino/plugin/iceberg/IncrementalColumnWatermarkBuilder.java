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
package io.trino.plugin.iceberg;

import io.airlift.log.Logger;
import io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.StoredWatermark;
import io.trino.spi.type.Int128;
import org.apache.iceberg.Metrics;

import java.util.Optional;

import static io.trino.plugin.iceberg.IncrementalColumnWatermarkReader.STORED_WATERMARK_CODEC;
import static java.util.Objects.requireNonNull;

public final class IncrementalColumnWatermarkBuilder
{
    private static final Logger log = Logger.get(IncrementalColumnWatermarkBuilder.class);

    private final IncrementalColumn incrementalColumn;
    private final Optional<Object> previousMaxValue;
    private Optional<Object> appendedMaxValue = Optional.empty();
    private boolean appendedMaxValueKnown = true;

    public IncrementalColumnWatermarkBuilder(IncrementalColumn incrementalColumn, Optional<Object> previousMaxValue)
    {
        this.incrementalColumn = requireNonNull(incrementalColumn, "incrementalColumn is null");
        this.previousMaxValue = requireNonNull(previousMaxValue, "previousMaxValue is null");
    }

    public void acceptFile(IcebergFileFormat fileFormat, Metrics fileMetrics)
    {
        if (!appendedMaxValueKnown) {
            return;
        }
        if (!incrementalColumn.hasExactUpperBoundIn(fileFormat)) {
            appendedMaxValueKnown = false;
            return;
        }
        Optional<Object> fileMaxValue = incrementalColumn.maxValueIn(fileMetrics);
        if (fileMaxValue.isPresent()) {
            appendedMaxValue = greaterOf(appendedMaxValue, fileMaxValue);
        }
        else {
            appendedMaxValueKnown = incrementalColumn.isNullInEveryRowOf(fileMetrics);
        }
    }

    public Optional<String> build()
    {
        if (!appendedMaxValueKnown) {
            log.debug("An appended file states no exact upper bound for incremental column %s, storing no watermark", incrementalColumn.name());
            return Optional.empty();
        }
        return greaterOf(previousMaxValue, appendedMaxValue).map(maxValue -> encodeWatermark(incrementalColumn, maxValue));
    }

    static String encodeWatermark(IncrementalColumn incrementalColumn, Object maxValue)
    {
        return STORED_WATERMARK_CODEC.toJson(new StoredWatermark(
                incrementalColumn.field().fieldId(),
                incrementalColumn.trinoType().getTypeId(),
                maxValue instanceof Int128 int128 ? int128.toBigInteger().toString() : Long.toString((Long) maxValue)));
    }

    static Object greaterOf(Object left, Object right)
    {
        if (left instanceof Int128 leftValue) {
            return leftValue.compareTo((Int128) right) >= 0 ? left : right;
        }
        return (Long) left >= (Long) right ? left : right;
    }

    private static Optional<Object> greaterOf(Optional<Object> left, Optional<Object> right)
    {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return Optional.of(greaterOf(left.orElseThrow(), right.orElseThrow()));
    }
}
