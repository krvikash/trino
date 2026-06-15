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
package io.starburst.stargate.tablemaintenance;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.StandardErrorCode;

import java.util.Set;

public class MaintenanceConstants
{
    public static final String ERROR_FETCHING_RESULTS = "Error fetching results";

    public static final Set<String> ERROR_CODE_NAMES_TO_REDUCE_OPTIMIZE_CHECKPOINTS = ImmutableSet.of(
            // IcebergErrorCode.ICEBERG_COMMIT_ERROR.name() — IcebergErrorCode is not a dependency of this module
            "ICEBERG_COMMIT_ERROR",
            StandardErrorCode.EXCEEDED_TIME_LIMIT.name(),
            ERROR_FETCHING_RESULTS);

    public static final Set<Integer> ERROR_CODES_TO_REDUCE_OPTIMIZE_CHECKPOINTS = ImmutableSet.of(
            // IcebergErrorCode.ICEBERG_COMMIT_ERROR.toErrorCode().getCode() — IcebergErrorCode is not a dependency of this module
            84148224 + 12,
            StandardErrorCode.EXCEEDED_TIME_LIMIT.toErrorCode().getCode());

    public static final Set<String> ERROR_CODE_NAMES_FOR_NESTED_PARTITION_OPTIMIZE = ImmutableSet.of(
            StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT.name(),
            StandardErrorCode.CLUSTER_OUT_OF_MEMORY.name());

    public static final Set<Integer> ERROR_CODES_FOR_NESTED_PARTITION_OPTIMIZE = ImmutableSet.of(
            StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT.toErrorCode().getCode(),
            StandardErrorCode.CLUSTER_OUT_OF_MEMORY.toErrorCode().getCode());

    private MaintenanceConstants() {}
}
