/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.client;

public enum ErrorCode
{
    // OVERLOADED, // node is overloaded // todo
    DRAINING, // node is draining
    INTERNAL_ERROR, // general internal error
    EXCHANGE_FINISHED, // exchange still exists but already finished
    EXCHANGE_NOT_FOUND, // exchange not found
    CHUNK_DRAINED, // chunk was drained and will not be served by this buffer node as the node is draining; request should be retried to another RUNNING buffer node.
    CHUNK_NOT_FOUND, // chunk not found
    USER_ERROR // unexpected input from the client
}
