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
package io.trino.operator.gpu;

/**
 * Score indicating the expected benefit of GPU acceleration for an operation.
 */
public enum GpuScore
        implements Comparable<GpuScore>
{
    // Ordered from the lowest score to the highest.

    /**
     * Can run on GPU, but not necessarily beneficial
     */
    POTENTIAL,

    /**
     * Expected to benefit from GPU
     */
    PREFERRED,
}
