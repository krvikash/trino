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
package io.trino.testing;

import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

import static java.nio.file.Files.createTempFile;

public final class FileUtils
{
    private FileUtils() {}

    /**
     * {@link FileUtils#createTempFileForTesting(String, String, FileAttribute[])}
     */
    public static Path createTempFileForTesting()
    {
        return createTempFileForTesting(null, null);
    }

    /**
     * Creates an empty temporary file that will be automatically deleted when the JVM stops.
     *
     * @return A Path object leading to the file created
     */
    public static Path createTempFileForTesting(@Nullable String prefix, @Nullable String suffix, FileAttribute<?>... attrs)
    {
        try {
            Path tmpFile = createTempFile(prefix, suffix, attrs);
            tmpFile.toFile().deleteOnExit();
            return tmpFile;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
