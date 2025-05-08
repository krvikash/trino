/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.inject.Inject;
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

    @Inject
    public FileBackedModelConnectionSpecsLoader(AiFileStorageConfig config)
    {
        requireNonNull(config, "config is null");
        this.path = Path.of(config.getModelConnectionSpecsFile());
    }

    @Override
    public ModelConnectionSpecs load()
    {
        try {
            String json = Files.readString(path);
            return parseJson(json, ModelConnectionSpecs.class);
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
