/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.synapse;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static com.starburstdata.trino.plugin.synapse.SynapseServer.TEST_SCHEMA;
import static java.lang.String.format;

public class TestSynapseInstanceCleaner
        implements TestExecutionListener
{
    private final SynapseServer synapseServer = new SynapseServer();

    private static final int ERROR_OBJECT_NOT_FOUND = 3701;

    private static final Logger LOG = Logger.get(TestSynapseInstanceCleaner.class);

    private static final ImmutableSet<String> OBJECT_TYPES = ImmutableSet.of("table", "view");

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan)
    {
        getAndLogObjectsCount();
        LOG.info("Identifying objects to drop...");
        for (String objectType : OBJECT_TYPES) {
            Collection<String> objectsToDrop = getObjectsToDrop(objectType);
            if (objectsToDrop.isEmpty()) {
                LOG.info("Not dropping any %ss", objectType);
                continue;
            }
            LOG.info("Identified %d %ss to drop: %s", objectsToDrop.size(), objectType, objectsToDrop);
            dropObjectsFrom(objectType, objectsToDrop);
        }
        if (getAndLogObjectsCount() == 0) {
            LOG.info("Dropping empty schema " + TEST_SCHEMA);
            synapseServer.execute("DROP SCHEMA " + TEST_SCHEMA);
        }
        synapseServer.close();
    }

    private void dropObjectsFrom(String objectType, Collection<String> objectsToDrop)
    {
        // Azure Synapse does not support "DROP obj IF EXISTS"
        for (String objectName : objectsToDrop) {
            synapseServer.executeIgnoringErrors(format("DROP %s %s.[%s]", objectType, TEST_SCHEMA, objectName), ERROR_OBJECT_NOT_FOUND);
        }
    }

    private String selectFromTestSchema(String query, String table)
    {
        return format(
                "SELECT %s FROM %s AS obj INNER JOIN sys.schemas AS s ON obj.schema_id = s.schema_id WHERE s.name = '%s'",
                query, table, TEST_SCHEMA);
    }

    private int getObjectCount()
    {
        return synapseServer.executeQuery(selectFromTestSchema("count(*)", "sys.objects"), resultSet -> {
            try {
                resultSet.next();
                return resultSet.getInt(1);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Collection<String> getObjectsToDrop(String objectType)
    {
        Collection<String> results = new ArrayList<>();

        return synapseServer.executeQuery(selectFromTestSchema("obj.name AS name", format("sys.%ss", objectType)),
                resultSet -> {
                try {
                    while (resultSet.next()) {
                        String name = resultSet.getString("name");
                        results.add(name);
                    }
                    return results;
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    /**
     * Log total number of objects in the test schema.
     */
    private int getAndLogObjectsCount()
    {
        int objectCount = getObjectCount();
        LOG.info("Schema '%s' contains %d objects.", TEST_SCHEMA, objectCount);
        return objectCount;
    }
}
