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

import java.util.Collection;
import java.util.Collections;

public class EmptyConfigProvider
        implements CatalogConfigProvider
{
    @Override
    public Collection<CatalogConfig> loadCatalogConfigs()
    {
        return Collections.emptyList();
    }
}
