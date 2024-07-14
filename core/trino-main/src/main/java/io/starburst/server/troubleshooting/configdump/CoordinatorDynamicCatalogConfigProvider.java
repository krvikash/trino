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

import com.google.inject.Inject;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;

import java.util.Collection;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CoordinatorDynamicCatalogConfigProvider
        implements CatalogConfigProvider
{
    private final CatalogStore catalogStore;

    @Inject
    public CoordinatorDynamicCatalogConfigProvider(CatalogStore catalogStore)
    {
        this.catalogStore = requireNonNull(catalogStore, "catalogStore is null");
    }

    @Override
    public Collection<CatalogConfig> loadCatalogConfigs()
    {
        return catalogStore.getCatalogs().stream()
                .map(catalog -> {
                    CatalogProperties catalogProperties = catalog.loadProperties();
                    return new CatalogConfig(catalog.name().toString(), catalogProperties.connectorName().toString(), catalogProperties.properties());
                })
                .collect(toImmutableList());
    }
}
