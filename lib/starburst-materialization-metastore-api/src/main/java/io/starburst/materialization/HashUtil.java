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
package io.starburst.materialization;

import io.airlift.slice.XxHash64;

import static java.nio.charset.StandardCharsets.UTF_8;

public class HashUtil
{
    private HashUtil() {}

    public static long combineHash(long previous, long value)
    {
        long x = previous + 0x9E3779B97F4A7C15L + Long.rotateLeft(value, 27);
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        return x ^ (x >>> 31);
    }

    public static long hash(String value)
    {
        return XxHash64.hash(value.getBytes(UTF_8));
    }
}
