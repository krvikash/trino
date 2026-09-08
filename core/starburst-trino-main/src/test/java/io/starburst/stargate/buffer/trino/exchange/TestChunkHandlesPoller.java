/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import io.starburst.stargate.buffer.data.client.DataApiException;
import io.starburst.stargate.buffer.data.client.ErrorCode;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.net.ConnectException;

import static io.starburst.stargate.buffer.trino.exchange.BufferServiceExchangeErrorCode.COMMUNICATION_FAILURE;
import static io.starburst.stargate.buffer.trino.exchange.ChunkHandlesPoller.toExchangeFailure;
import static io.trino.spi.StandardErrorCode.EXCHANGE_DATA_UNRECOVERABLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestChunkHandlesPoller
{
    @Test
    public void testNodeLossIsUnrecoverableExchangeDataLoss()
    {
        // a data node that stopped answering took every chunk it held for the exchange with it
        assertUnrecoverable(new UncheckedIOException(new ConnectException("Connection refused")));
        assertUnrecoverable(new RuntimeException("request timed out"));
        assertUnrecoverable(new DataApiException(ErrorCode.INTERNAL_ERROR, "cannot connect to node"));
        assertUnrecoverable(new DataApiException(ErrorCode.BUFFER_NODE_NOT_FOUND, "Buffer node 7 not found"));
        assertUnrecoverable(new DataApiException(ErrorCode.BUFFER_NODE_GONE, "Buffer node 7 left the cluster without draining"));
        // the node is alive but no longer has a usable copy of the exchange
        assertUnrecoverable(new DataApiException(ErrorCode.EXCHANGE_NOT_FOUND, "exchange not found"));
        assertUnrecoverable(new DataApiException(ErrorCode.EXCHANGE_CORRUPTED, "exchange corrupted"));
        assertUnrecoverable(new DataApiException(ErrorCode.CHUNK_NOT_FOUND, "chunk not found"));
    }

    @Test
    public void testFailuresWithDataIntactStayCommunicationFailures()
    {
        // draining nodes hand their chunks over to spooling storage, an overloaded node still has them
        assertCommunicationFailure(new DataApiException(ErrorCode.DRAINING, "draining"));
        assertCommunicationFailure(new DataApiException(ErrorCode.DRAINED, "drained"));
        assertCommunicationFailure(new DataApiException(ErrorCode.DRAINING_ON_RETRY, "draining on retry"));
        assertCommunicationFailure(new DataApiException(ErrorCode.OVERLOADED, "overloaded"));
        assertCommunicationFailure(new DataApiException(ErrorCode.EXCHANGE_FINISHED, "finished"));
        assertCommunicationFailure(new DataApiException(ErrorCode.USER_ERROR, "bad request"));
    }

    @Test
    public void testMessageAndCauseArePreserved()
    {
        DataApiException cause = new DataApiException(ErrorCode.INTERNAL_ERROR, "cannot connect to node");
        TrinoException exception = toExchangeFailure("Error listing closed chunks exchange e in data node 7", cause);
        assertThat(exception).hasMessage("Error listing closed chunks exchange e in data node 7");
        assertThat(exception).hasCause(cause);
    }

    private static void assertUnrecoverable(Throwable failure)
    {
        assertThat(toExchangeFailure("message", failure).getErrorCode()).isEqualTo(EXCHANGE_DATA_UNRECOVERABLE.toErrorCode());
    }

    private static void assertCommunicationFailure(Throwable failure)
    {
        assertThat(toExchangeFailure("message", failure).getErrorCode()).isEqualTo(COMMUNICATION_FAILURE.toErrorCode());
    }
}
