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
package io.starburst.stargate.tablemaintenance.partitioned;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.stargate.tablemaintenance.MaintenanceQueryGenerators.singleQuoteEscape;
import static io.starburst.stargate.tablemaintenance.partitioned.FileType.determineFileType;
import static java.util.Objects.requireNonNull;

public record UnoptimizedPartitionValue(
        List<ColumnValue> columnValues,
        FileType fileType)
{
    public UnoptimizedPartitionValue
    {
        columnValues = ImmutableList.copyOf(requireNonNull(columnValues, "columnValues is null"));
        requireNonNull(fileType, "fileType is null");
        checkArgument(!columnValues.isEmpty(), "columnValues must not be empty");
    }

    public static UnoptimizedPartitionValue of(FileType fileType, ColumnValue... columnValues)
    {
        return new UnoptimizedPartitionValue(Arrays.asList(columnValues), fileType);
    }

    public static UnoptimizedPartitionValue unoptimizedPartitionValue(String partitionColumnValue, String partitionDataType, String fileTypeRaw)
    {
        return new UnoptimizedPartitionValue(
                ImmutableList.of(new ColumnValue(partitionColumnValue, partitionDataType)),
                determineFileType(fileTypeRaw));
    }

    public record ColumnValue(String value, String dataType)
    {
        public ColumnValue
        {
            requireNonNull(value, "value is null");
            requireNonNull(dataType, "dataType is null");
        }

        public String getQueryValue()
        {
            return "CAST('" + singleQuoteEscape(value) + "' AS " + dataType + ")";
        }
    }
}
