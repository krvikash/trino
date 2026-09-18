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
package io.trino.operator.gpu.join;

public final class GpuJoinUtils
{
    private GpuJoinUtils() {}

    /// Device memory a join output chunk is aimed at when the output does not fit a single chunk.
    public static final long TARGET_OUTPUT_CHUNK_BYTES = 128L * 1024 * 1024;
    /// Device memory a join output chunk may not exceed. Higher than [#TARGET_OUTPUT_CHUNK_BYTES]
    /// because a chunk sized from this bound is not known to fit exactly, unlike one sized from a
    /// page's average row.
    public static final long MAX_OUTPUT_CHUNK_BYTES = 1024L * 1024 * 1024;
}
