/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client.spooling;

/**
 * Selects which client-side spooled-chunk reader implementation to use. Mirrors the server-side
 * {@code SpoolingStorageDriver} so the public configuration surface is symmetric and extensible
 * without a {@code @LegacyConfig} migration when additional drivers are introduced.
 */
public enum SpoolingClientDriver
{
    /**
     * Use the per-scheme native readers picked by {@code spooling-storage-type}.
     */
    NATIVE,
    /**
     * Read spooled chunks through the generic {@link io.trino.filesystem.TrinoFileSystem}
     * abstraction.
     */
    TRINO_FS,
}
