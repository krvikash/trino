/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor.filetracker;

import io.starburst.schema.discovery.formats.lakehouse.LakehouseFormat;
import io.starburst.schema.discovery.processor.Processor.ProcessorPath;
import io.trino.filesystem.Location;

import java.util.Optional;

public interface FileTracker
{
    boolean shouldStopAddingTables(Location potentialTablePath);

    boolean hasEnoughSampledTables(Location directoryPath);

    boolean hasEnoughSamplesForTable(Location tablePath);

    Optional<ProcessorPath> getNextSampleFileForTable(Location directoryPath, Location filePath, Optional<LakehouseFormat> lakehouseFormat);
}
