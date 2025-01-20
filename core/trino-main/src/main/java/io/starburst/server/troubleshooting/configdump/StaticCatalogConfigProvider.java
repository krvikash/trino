/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.configdump;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.connector.StaticCatalogManagerConfig;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.connector.CatalogConfigurationReader.CONNECTOR_NAME_PROPERTY;
import static com.starburstdata.presto.connector.CatalogConfigurationReader.loadCatalogProperties;
import static java.util.Objects.requireNonNull;

public class StaticCatalogConfigProvider
        implements CatalogConfigProvider
{
    private final Path catalogConfigurationDir;
    private final List<String> disabledCatalogs;

    @Inject
    public StaticCatalogConfigProvider(StaticCatalogManagerConfig staticCatalogManagerConfig)
    {
        requireNonNull(staticCatalogManagerConfig, "staticCatalogManagerConfig is null");
        this.catalogConfigurationDir = requireNonNull(staticCatalogManagerConfig.getCatalogConfigurationDir(), "catalogConfigurationDir is null").toPath();
        this.disabledCatalogs = firstNonNull(staticCatalogManagerConfig.getDisabledCatalogs(), ImmutableList.of());
    }

    @Override
    public Collection<CatalogConfig> loadCatalogConfigs()
    {
        return loadCatalogProperties(catalogConfigurationDir, disabledCatalogs).entrySet().stream()
                .map(entry -> {
                    Map<String, String> catalogProperties = entry.getValue();
                    return new CatalogConfig(entry.getKey(), catalogProperties.get(CONNECTOR_NAME_PROPERTY), catalogProperties);
                })
                .collect(toImmutableList());
    }
}
