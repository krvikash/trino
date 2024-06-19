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
package io.trino.plugin.warp.util;

import java.nio.charset.StandardCharsets;

import static com.google.common.hash.Hashing.farmHashFingerprint64;

public class StorageUtils
{
    private StorageUtils()
    {
    }

    public static long fileHash64(String rowGroupFilePath)
    {
        return farmHashFingerprint64().hashString(rowGroupFilePath, StandardCharsets.UTF_8).asLong();
    }
}
