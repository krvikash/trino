/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.disk;

import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public record DiskChunkSlot(Path file, DiskSpaceLease lease, Runnable diskRelease)
{
    public DiskChunkSlot
    {
        requireNonNull(file, "file is null");
        requireNonNull(lease, "lease is null");
        requireNonNull(diskRelease, "diskRelease is null");
    }
}
