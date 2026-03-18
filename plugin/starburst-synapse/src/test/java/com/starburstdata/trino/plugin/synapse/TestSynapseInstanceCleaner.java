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

import com.google.common.collect.ImmutableSetMultimap;
import io.airlift.log.Logger;
import io.trino.tpch.TpchTable;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import static com.starburstdata.trino.plugin.synapse.SynapseQueryRunner.TEST_SCHEMA;
import static java.lang.String.format;

public class TestSynapseInstanceCleaner
        implements TestExecutionListener
{
    private final SynapseServer synapseServer = new SynapseServer();

    private static final int ERROR_OBJECT_NOT_FOUND = 3701;

    private static final Logger LOG = Logger.get(TestSynapseInstanceCleaner.class);

    private static final ImmutableSetMultimap<String, String> OBJECTS_TO_KEEP;

    static {
        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.<String, String>builder()
                .put("view", "user_context");
        for (TpchTable<?> table : TpchTable.getTables()) {
            builder.put("table", table.getTableName().toLowerCase(Locale.ENGLISH));
        }
        OBJECTS_TO_KEEP = builder.build();
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan)
    {
        logObjectsCount();
        LOG.info("Identifying objects to drop...");
        for (Map.Entry<String, Collection<String>> entry : OBJECTS_TO_KEEP.asMap().entrySet()) {
            String objectType = entry.getKey();
            Collection<String> objectsToKeep = entry.getValue();
            if (!objectsToKeep.isEmpty()) {
                LOG.info("Never drop these %ss: %s", objectType, objectsToKeep);
            }
            Collection<String> objectsToDrop = getObjectsToDrop(format("sys.%ss", objectType), objectsToKeep);
            if (objectsToDrop.isEmpty()) {
                LOG.info("Not dropping any %ss", objectType);
                continue;
            }
            LOG.info("Identified %d %ss to drop: %s", objectsToDrop.size(), objectType, objectsToDrop);
            dropObjectsFrom(objectType, objectsToDrop);
        }
        logObjectsCount();
        synapseServer.close();
    }

    private void dropObjectsFrom(String objectType, Collection<String> objectsToDrop)
    {
        // Azure Synapse does not support "DROP obj IF EXISTS"
        for (String objectName : objectsToDrop) {
            synapseServer.executeIgnoringErrors(format("DROP %s %s.[%s]", objectType, TEST_SCHEMA, objectName), ERROR_OBJECT_NOT_FOUND);
        }
    }

    private int getObjectCount()
    {
        return synapseServer.executeQuery("SELECT count(*) FROM sys.objects", resultSet -> {
            try {
                resultSet.next();
                return resultSet.getInt(1);
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Collection<String> getObjectsToDrop(String objectType, Collection<String> objectsToKeep)
    {
        Collection<String> results = new ArrayList<>();

        return synapseServer.executeQuery(
                format("SELECT name FROM sys.%ss WHERE DATEDIFF(day, create_date, GETUTCDATE()) > 1", objectType),
                resultSet -> {
                try {
                    while (resultSet.next()) {
                        String name = resultSet.getString("name");
                        if (!objectsToKeep.contains(name)) {
                            results.add(name);
                        }
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
    private void logObjectsCount()
    {
        int tableCount = getObjectCount();
        LOG.info("Schema '%s' contains %d objects.", TEST_SCHEMA, tableCount);
    }
}
