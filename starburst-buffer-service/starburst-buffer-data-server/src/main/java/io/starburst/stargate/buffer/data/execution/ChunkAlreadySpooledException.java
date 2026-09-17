/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

// Internal signal that a chunk was already spooled but its entry is not yet visible in
// SpooledChunksByExchange. Not wire-facing; ChunkManager converts this to CHUNK_NOT_FOUND.
public class ChunkAlreadySpooledException
        extends RuntimeException
{
    public ChunkAlreadySpooledException(String message)
    {
        super(message);
    }
}
