/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationLoader;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.util.Objects.requireNonNull;

public final class CatalogConfigurationReader
{
    private static final Logger log = Logger.get(CatalogConfigurationReader.class);

    public static final String CONNECTOR_NAME_PROPERTY = "connector.name";

    private CatalogConfigurationReader() {}

    public static Map<String, Map<String, String>> loadCatalogProperties(Path catalogConfigurationDir, List<String> disabledCatalogs)
    {
        requireNonNull(catalogConfigurationDir, "catalogConfigurationDir is null");
        requireNonNull(disabledCatalogs, "disabledCatalogs is null");

        ImmutableMap.Builder<String, Map<String, String>> builder = ImmutableMap.builder();
        for (Path file : listCatalogFiles(catalogConfigurationDir)) {
            String catalogName = getNameWithoutExtension(file.getFileName().toString());
            if (disabledCatalogs.contains(catalogName)) {
                continue;
            }

            try {
                Map<String, String> properties = ConfigurationLoader.loadPropertiesFrom(file.toString());
                builder.put(catalogName, properties);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading catalog property file " + file, e);
            }
        }
        return builder.buildOrThrow();
    }

    private static List<Path> listCatalogFiles(Path catalogsDirectory)
    {
        try (Stream<Path> files = Files.list(catalogsDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(file -> file.getFileName().toString().endsWith(".properties"))
                    .collect(toImmutableList());
        }
        catch (NoSuchFileException e) {
            return ImmutableList.of();
        }
        catch (IOException e) {
            log.warn(e, "Error listing catalog configurations from the directory: %s", catalogsDirectory);
            return ImmutableList.of();
        }
    }
}
