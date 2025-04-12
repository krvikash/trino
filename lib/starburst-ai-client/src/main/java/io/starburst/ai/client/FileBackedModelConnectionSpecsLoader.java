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
package io.starburst.ai.client;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Throwables.getCausalChain;
import static io.starburst.ai.client.AiClientErrorCode.AI_CLIENT_ERROR;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;

public class FileBackedModelConnectionSpecsLoader
        implements ModelConnectionSpecsLoader
{
    private final Path path;
    private final SecretsResolver secretsResolver;

    @Inject
    public FileBackedModelConnectionSpecsLoader(AiFileStorageConfig config, SecretsResolver secretsResolver)
    {
        requireNonNull(config, "config is null");
        this.path = Path.of(config.getModelConnectionSpecsFile());
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    @Override
    public ModelConnectionSpecs load()
    {
        try {
            String json = Files.readString(path);
            String resolvedJson = secretsResolver.getResolvedConfiguration(ImmutableMap.of("json", json)).get("json");
            return parseJson(resolvedJson, ModelConnectionSpecs.class);
        }
        catch (RuntimeException e) {
            // the error message can contain sensitive information, so just include the location of the parsing error
            getCausalChain(e).stream()
                    .filter(JsonParseException.class::isInstance)
                    .map(JsonParseException.class::cast)
                    .findFirst()
                    .ifPresent(jpe -> {
                        throw new TrinoException(AI_CLIENT_ERROR, "Error parsing AI model connection spec at: %s".formatted(jpe.getLocation().offsetDescription()));
                    });
            throw e;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
