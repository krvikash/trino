/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.spi.security.ConnectorIdentity;

import static com.google.common.base.Verify.verify;
import static java.util.Locale.ENGLISH;

public class SalesforceCdpIdentifierMapping
        implements IdentifierMapping
{
    private static final String TABLE_SUFFIX = "__dll";
    private static final String COLUMN_SUFFIX = "__c";

    @Override
    public String fromRemoteSchemaName(String remoteSchemaName)
    {
        return remoteSchemaName.toLowerCase(ENGLISH);
    }

    @Override
    public String fromRemoteTableName(String remoteSchemaName, String remoteTableName)
    {
        // todo there are table ends with __dml as well, check when such tables are created and if we require integration with it
        if (remoteTableName.endsWith(TABLE_SUFFIX)) {
            remoteTableName = remoteTableName.substring(0, remoteTableName.lastIndexOf(TABLE_SUFFIX));
        }
        return remoteTableName.toLowerCase(ENGLISH);
    }

    @Override
    public String fromRemoteColumnName(String remoteColumnName)
    {
        verify(remoteColumnName.endsWith(COLUMN_SUFFIX), "remoteColumnName must be suffixed with '%s'", COLUMN_SUFFIX);
        remoteColumnName = remoteColumnName.substring(0, remoteColumnName.lastIndexOf(COLUMN_SUFFIX));
        return remoteColumnName.toLowerCase(ENGLISH);
    }

    @Override
    public String toRemoteSchemaName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String schemaName)
    {
        return toRemoteIdentifier(schemaName, remoteIdentifiers);
    }

    @Override
    public String toRemoteTableName(RemoteIdentifiers remoteIdentifiers, ConnectorIdentity identity, String remoteSchema, String tableName)
    {
        tableName = tableName + TABLE_SUFFIX;
        return toRemoteIdentifier(tableName, remoteIdentifiers);
    }

    @Override
    public String toRemoteColumnName(RemoteIdentifiers remoteIdentifiers, String columnName)
    {
        columnName = columnName + COLUMN_SUFFIX;
        return toRemoteIdentifier(columnName, remoteIdentifiers);
    }

    private String toRemoteIdentifier(String identifier, RemoteIdentifiers remoteIdentifiers)
    {
        if (remoteIdentifiers.storesUpperCaseIdentifiers()) {
            return identifier.toUpperCase(ENGLISH);
        }
        return identifier;
    }
}
