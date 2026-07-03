/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.trino.hive.thrift.metastore.Table;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThriftTransportMaxMessageSize
{
    /**
     * Verifies that a single field whose declared size exceeds the configured limit triggers
     * "Message size exceeds limit". The limit is enforced by TBinaryProtocol calling
     * checkReadBytesAvailable(fieldLength) against TIOStreamTransport's remainingMessageSize.
     */
    @Test
    public void testSingleFieldExceedingLimitIsRejected()
            throws Exception
    {
        // One parameter value of 1000 bytes
        Table table = createTableWithSingleParameter("test_table", 1000);
        byte[] serialized = serializeTable(table);

        // Limit smaller than the parameter value: must fail.
        TIOStreamTransport smallTransport = new TIOStreamTransport(config(500), new ByteArrayInputStream(serialized));
        TBinaryProtocol smallProtocol = new TBinaryProtocol(smallTransport);
        smallProtocol.readMessageBegin();
        Table partialTable = new Table();
        assertThatThrownBy(() -> partialTable.read(smallProtocol))
                .isInstanceOf(TTransportException.class)
                .hasMessageContaining("Message size exceeds limit");

        // Limit larger than the parameter value: must succeed.
        TIOStreamTransport largeTransport = new TIOStreamTransport(config(2000), new ByteArrayInputStream(serialized));
        TBinaryProtocol largeProtocol = new TBinaryProtocol(largeTransport);
        largeProtocol.readMessageBegin();
        Table readTable = new Table();
        readTable.read(largeProtocol);

        assertThat(readTable.getTableName()).isEqualTo(table.getTableName());
        assertThat(readTable.getDbName()).isEqualTo(table.getDbName());
        assertThat(readTable.getParameters()).isEqualTo(table.getParameters());
    }

    /**
     * Reproduces ENG-17546: a single field whose size exceeds a configured limit can be read
     * after raising the limit. Uses a small proxy value to avoid allocating ~300 MB; the
     * enforcement mechanism is identical regardless of the actual threshold.
     */
    @Test
    public void testRaisedLimitAllowsFieldExceedingThriftDefault()
            throws Exception
    {
        int threshold = 1000;
        Table table = createTableWithSingleParameter("large_table", threshold + 1);
        byte[] serialized = serializeTable(table);

        // At the threshold: must fail because the field length alone exceeds it.
        TIOStreamTransport defaultTransport = new TIOStreamTransport(
                config(threshold),
                new ByteArrayInputStream(serialized));
        TBinaryProtocol defaultProtocol = new TBinaryProtocol(defaultTransport);
        defaultProtocol.readMessageBegin();
        Table partialTable = new Table();
        assertThatThrownBy(() -> partialTable.read(defaultProtocol))
                .isInstanceOf(TTransportException.class)
                .hasMessageContaining("Message size exceeds limit");

        // With a raised limit: must succeed.
        TIOStreamTransport raisedTransport = new TIOStreamTransport(
                config(threshold + 1000),
                new ByteArrayInputStream(serialized));
        TBinaryProtocol raisedProtocol = new TBinaryProtocol(raisedTransport);
        raisedProtocol.readMessageBegin();
        Table readTable = new Table();
        readTable.read(raisedProtocol);

        assertThat(readTable.getTableName()).isEqualTo(table.getTableName());
        assertThat(readTable.getDbName()).isEqualTo(table.getDbName());
    }

    /**
     * Verifies that the TSocket-based setup in Transport.createRaw — setMaxMessageSize +
     * updateKnownMessageSize(0) — correctly applies the configured limit to a real socket.
     */
    @Test
    public void testTSocketLimitIsApplied()
            throws Exception
    {
        int limit = 500;
        try (ServerSocket serverSocket = new ServerSocket(0);
                Socket socket = new Socket("localhost", serverSocket.getLocalPort())) {
            serverSocket.accept().close();

            TSocket tSocket = new TSocket(socket);
            tSocket.setMaxMessageSize(limit);
            tSocket.updateKnownMessageSize(0);

            assertThat(tSocket.getConfiguration().getMaxMessageSize()).isEqualTo(limit);
            tSocket.checkReadBytesAvailable(limit);
            assertThatThrownBy(() -> tSocket.checkReadBytesAvailable(limit + 1L))
                    .isInstanceOf(TTransportException.class)
                    .hasMessageContaining("Message size exceeds limit");
        }
    }

    /**
     * Verifies that Transport.create propagates maxMessageSizeBytes to the underlying TSocket so
     * that reading a payload exceeding that limit over a real TCP connection is rejected.
     */
    @Test
    public void testTransportCreateEnforcesMessageSizeLimit()
            throws Exception
    {
        int maxMessageSizeBytes = 500;
        int valueLength = 2 * maxMessageSizeBytes;
        byte[] payload = serializeTable(createTableWithSingleParameter("test_table", valueLength));
        assertThat(payload.length).isGreaterThan(maxMessageSizeBytes);

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Thread serverThread = startPayloadServer(serverSocket, payload);

            try (TTransport transport = createTransport(serverSocket, maxMessageSizeBytes)) {
                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                protocol.readMessageBegin();
                assertThatThrownBy(() -> new Table().read(protocol))
                        .isInstanceOf(TTransportException.class)
                        .hasMessageContaining("Message size exceeds limit");
            }

            serverThread.join(5_000);
            assertThat(serverThread.isAlive()).as("server thread did not finish in time").isFalse();
        }
    }

    @Test
    public void testTransportCreateAllowsMessageWithinLimit()
            throws Exception
    {
        int valueLength = 1000;
        Table original = createTableWithSingleParameter("test_table", valueLength);
        byte[] payload = serializeTable(original);
        int maxMessageSizeBytes = payload.length * 2;
        assertThat(payload.length).isLessThan(maxMessageSizeBytes);

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            Thread serverThread = startPayloadServer(serverSocket, payload);

            try (TTransport transport = createTransport(serverSocket, maxMessageSizeBytes)) {
                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                protocol.readMessageBegin();
                Table readTable = new Table();
                readTable.read(protocol);

                assertThat(readTable.getTableName()).isEqualTo(original.getTableName());
                assertThat(readTable.getDbName()).isEqualTo(original.getDbName());
                assertThat(readTable.getParameters()).isEqualTo(original.getParameters());
            }

            serverThread.join(5_000);
            assertThat(serverThread.isAlive()).as("server thread did not finish in time").isFalse();
        }
    }

    @Test
    public void testLimitIsEnforced()
            throws TTransportException
    {
        int limit = 100;
        TIOStreamTransport transport = new TIOStreamTransport(config(limit), new ByteArrayInputStream(new byte[0]));

        // A check within the limit succeeds.
        transport.checkReadBytesAvailable(limit);

        // A check exceeding the limit throws.
        assertThatThrownBy(() -> transport.checkReadBytesAvailable(limit + 1L))
                .isInstanceOf(TTransportException.class)
                .hasMessageContaining("Message size exceeds limit");
    }

    private static Thread startPayloadServer(ServerSocket serverSocket, byte[] payload)
    {
        return Thread.ofVirtual().start(() -> {
            try (Socket accepted = serverSocket.accept()) {
                accepted.getOutputStream().write(payload);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static TTransport createTransport(ServerSocket serverSocket, int maxMessageSizeBytes)
            throws TTransportException
    {
        return Transport.create(
                HostAndPort.fromParts("localhost", serverSocket.getLocalPort()),
                Optional.empty(),
                Optional.empty(),
                1_000,
                5_000,
                new NoHiveMetastoreAuthentication(),
                Optional.empty(),
                maxMessageSizeBytes);
    }

    private static byte[] serializeTable(Table table)
            throws Exception
    {
        TMemoryBuffer buffer = new TMemoryBuffer(0);
        TBinaryProtocol protocol = new TBinaryProtocol(buffer);
        protocol.writeMessageBegin(new TMessage("get_table", TMessageType.REPLY, 0));
        table.write(protocol);
        protocol.writeMessageEnd();
        return Arrays.copyOf(buffer.getArray(), buffer.length());
    }

    private static TConfiguration config(int maxMessageSize)
    {
        return new TConfiguration(maxMessageSize, TConfiguration.DEFAULT_MAX_FRAME_SIZE, TConfiguration.DEFAULT_RECURSION_DEPTH);
    }

    private static Table createTableWithSingleParameter(String name, int valueLength)
    {
        Table table = new Table();
        table.setTableName(name);
        table.setDbName("test_db");
        table.setParameters(ImmutableMap.of("key", "x".repeat(valueLength)));
        return table;
    }
}
