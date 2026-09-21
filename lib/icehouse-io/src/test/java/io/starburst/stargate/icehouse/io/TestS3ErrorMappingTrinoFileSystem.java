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
package io.starburst.stargate.icehouse.io;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.encryption.EncryptionKey;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Set;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

final class TestS3ErrorMappingTrinoFileSystem
{
    @Test
    void testEverythingImplemented()
            throws NoSuchMethodException
    {
        // These default methods are intentionally not overridden: their default implementations
        // are composed from other TrinoFileSystem methods (newInputFile, newOutputFile, listFiles)
        // that S3ErrorMappingTrinoFileSystem already overrides, so error mapping still applies.
        Set<Method> exclusions = ImmutableSet.of(
                TrinoFileSystem.class.getMethod("newEncryptedInputFile", Location.class, EncryptionKey.class),
                TrinoFileSystem.class.getMethod("newEncryptedInputFile", Location.class, long.class, EncryptionKey.class),
                TrinoFileSystem.class.getMethod("newEncryptedInputFile", Location.class, long.class, Instant.class, EncryptionKey.class),
                TrinoFileSystem.class.getMethod("newEncryptedOutputFile", Location.class, EncryptionKey.class),
                TrinoFileSystem.class.getMethod("listFilesStartingFrom", Location.class, String.class));
        assertAllMethodsOverridden(TrinoFileSystem.class, S3ErrorMappingTrinoFileSystem.class, exclusions);
    }
}
