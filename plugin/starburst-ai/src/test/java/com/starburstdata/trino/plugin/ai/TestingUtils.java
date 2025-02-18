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
package com.starburstdata.trino.plugin.ai;

import io.trino.Session;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestingUtils
{
    public static final Session TEST_AI_SESSION = testSessionBuilder()
            .setCatalog("ai")
            .build();

    private TestingUtils() {}

    public static File createModelConnectionSpecsFile(String content)
    {
        try {
            File tempFile = File.createTempFile("model_connection_specs_", ".json");
            tempFile.deleteOnExit();
            try (Writer writer = Files.newBufferedWriter(tempFile.toPath())) {
                writer.write(content);
            }
            return tempFile;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
