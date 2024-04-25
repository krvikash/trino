/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.spooling.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.io.LittleEndianDataInputStream;
import io.starburst.stargate.buffer.data.client.DataPage;
import io.starburst.stargate.buffer.data.client.spooling.SpooledChunk;
import io.starburst.stargate.buffer.data.execution.Chunk;
import io.starburst.stargate.buffer.data.execution.ChunkDataLease;
import org.apache.commons.io.input.BoundedInputStream;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.starburst.stargate.buffer.data.client.spooling.SpoolUtils.toDataPages;
import static io.starburst.stargate.buffer.data.execution.ChunkTestHelper.toChunkDataLease;
import static io.starburst.stargate.buffer.data.spooling.s3.MergedChunkDataAsyncRequestBody.fromChunks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestMergedChunkDataAsyncRequestBody
{
    // Allow sliceString to be explicitly altered for negative test
    private static Set<List<DataPage>> createDataPageList(long chunkId, int numSlices, int testStringAdjustment)
    {
        ImmutableSet.Builder<List<DataPage>> sliceBuilder = ImmutableSet.builder();
        for (int i = 0; i < numSlices; i++) {
            sliceBuilder.add(List.of(new DataPage(i, 0, utf8Slice("chunk " + chunkId + " testString " + (i + testStringAdjustment)))));
        }
        return sliceBuilder.build();
    }

    private static Set<List<DataPage>> createDataPageList(long chunkId, int numSlices)
    {
        return createDataPageList(chunkId, numSlices, 0);
    }

    private static Comparator<Chunk> chunkComparator = Comparator.comparingLong(Chunk::getChunkId);

    // Creates an ordered Map so that the slice counts are ordered as expected during serialization
    private static Map<Chunk, Set<List<DataPage>>> createChunkToSliceMap(List<Integer> sliceCounts)
    {
        TreeMap<Chunk, Set<List<DataPage>>> sortedMap = new TreeMap<>(chunkComparator);
        long chunkId = 0L;
        for (int sliceCount : sliceCounts) {
            sortedMap.put(new Chunk(chunkId), createDataPageList(chunkId, sliceCount));
            chunkId++;
        }
        return Collections.unmodifiableSortedMap(sortedMap);
    }

    private static Map<Chunk, ChunkDataLease> createChunkToLeaseMap(Map<Chunk, Set<List<DataPage>>> chunkToSliceMap)
    {
        TreeMap<Chunk, ChunkDataLease> newMap = chunkToSliceMap.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                (Map.Entry<Chunk, Set<List<DataPage>>> e) -> toChunkDataLease(e.getValue()),
                (l, l1) -> l,
                () -> new TreeMap<>(chunkComparator)));
        return Collections.unmodifiableSortedMap(newMap);
    }

    private static class MockSubscriber
            implements Subscriber<ByteBuffer>
    {
        private int onNextCounter;
        private int requestCounter;
        private boolean onCompleteCalled;
        private Subscription subscription;
        private final int onNextsPerRequest;
        private final boolean nestedRequests;
        private final ByteBuffer outputBuffer = ByteBuffer.allocate(1000);

        MockSubscriber(int onNextsPerRequest, boolean nestedRequests)
        {
            this.onNextsPerRequest = onNextsPerRequest;
            this.nestedRequests = nestedRequests;
        }

        @Override
        public void onSubscribe(Subscription subscription)
        {
            this.subscription = subscription;
            while (!onCompleteCalled) {
                requestCounter++;
                subscription.request(onNextsPerRequest);
            }
        }

        @Override
        public void onNext(ByteBuffer byteBuffer)
        {
            outputBuffer.put(byteBuffer);
            onNextCounter++;
            if (nestedRequests && !onCompleteCalled && onNextCounter % onNextsPerRequest == 0) {
                requestCounter++;
                subscription.request(onNextsPerRequest);
            }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onComplete()
        {
            if (onCompleteCalled) {
                fail("onComplete called multiple times");
            }
            onCompleteCalled = true;
            outputBuffer.flip();
        }

        public int getOnNextCounter()
        {
            return onNextCounter;
        }

        public int getRequestCounter()
        {
            return requestCounter;
        }

        public boolean isOnCompleteCalled()
        {
            return onCompleteCalled;
        }

        public byte[] getWrittenRawData()
        {
            assertTrue(onCompleteCalled);
            byte[] readBytes = new byte[outputBuffer.limit()];
            outputBuffer.get(readBytes);
            return readBytes;
        }
    }

    private Map<Chunk, Set<List<DataPage>>> parseOutput(Map<Long, SpooledChunk> spooledChunkMap, byte[] writtenRawData)
    {
        ImmutableMap.Builder<Chunk, Set<List<DataPage>>> readData = new ImmutableSortedMap.Builder<>(chunkComparator);
        InputStream rawInputStream = new ByteArrayInputStream(writtenRawData);
        for (Map.Entry<Long, SpooledChunk> e : spooledChunkMap.entrySet()) {
            try {
                rawInputStream.skip(e.getValue().offset());
                LittleEndianDataInputStream inputStream = new LittleEndianDataInputStream(BoundedInputStream.builder().setInputStream(rawInputStream).setMaxCount(e.getValue().length()).get());
                List<DataPage> dataPages = toDataPages(inputStream, true);
                readData.put(new Chunk(e.getKey()), dataPages.stream().collect(Collectors.mapping(ent -> ImmutableList.of(ent), toImmutableSet())));
                rawInputStream.reset();
            }
            catch (IOException ex) {
                fail("SpooledChunkMap not specified correctly");
            }
        }
        return readData.buildOrThrow();
    }

    private void verifyExpectedOutput(Map<Chunk, Set<List<DataPage>>> expectedOutput, Map<Long, SpooledChunk> spooledChunkMap, byte[] writtenRawData)
    {
        assertThat(parseOutput(spooledChunkMap, writtenRawData)).isEqualTo(expectedOutput);
    }

    private void verifyUnexpectedOutput(Map<Chunk, Set<List<DataPage>>> unexpectedOutput, Map<Long, SpooledChunk> spooledChunkMap, byte[] writtenRawData)
    {
        assertThat(parseOutput(spooledChunkMap, writtenRawData)).isNotEqualTo(unexpectedOutput);
    }

    @Test
    public void testVerifyOutputFailsForDifferentDataPage()
    {
        final int numSlices = 1;
        final int consumerCallLimit = 2;
        ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = ImmutableMap.builder();
        // This test may fail for multiple chunks because the key ordering will be different with HashMaps (Chunk vs Long).
        Map<Chunk, Set<List<DataPage>>> dataToWrite = ImmutableMap.of(new Chunk(0L), createDataPageList(0L, numSlices));
        Map<Chunk, Set<List<DataPage>>> dataWithDifferentTestString = ImmutableMap.of(new Chunk(0L), createDataPageList(0L, numSlices, 1));
        AsyncRequestBody testBody = fromChunks(
                "test",
                dataToWrite.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> toChunkDataLease(e.getValue()))),
                100,
                spooledChunkMap);
        MockSubscriber subscriber = new MockSubscriber(consumerCallLimit, false);
        testBody.subscribe(subscriber);
        byte[] writtenRawData = subscriber.getWrittenRawData();
        Map<Long, SpooledChunk> spooledChunks = spooledChunkMap.buildOrThrow();
        verifyExpectedOutput(dataToWrite, spooledChunks, writtenRawData);
        verifyUnexpectedOutput(dataWithDifferentTestString, spooledChunks, writtenRawData);
    }

    @Test
    public void testSimple()
    {
        final int numSlices = 2;
        final int consumerCallLimit = 1;
        ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = ImmutableMap.builder();
        Map<Chunk, Set<List<DataPage>>> dataToWrite = ImmutableMap.of(new Chunk(0L), createDataPageList(0L, numSlices));
        AsyncRequestBody testBody = fromChunks(
                "test",
                dataToWrite.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> toChunkDataLease(e.getValue()))),
                100,
                spooledChunkMap);
        MockSubscriber subscriber = new MockSubscriber(consumerCallLimit, false);
        testBody.subscribe(subscriber);
        assertTrue(subscriber.isOnCompleteCalled());
        assertEquals(numSlices + 1, subscriber.getOnNextCounter());
        assertEquals(numSlices + 1, subscriber.getRequestCounter());
        verifyExpectedOutput(dataToWrite, spooledChunkMap.buildOrThrow(), subscriber.getWrittenRawData());
    }

    @Test
    public void testSimpleNested()
    {
        final int numSlices = 2;
        final int consumerCallLimit = 1;
        ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = ImmutableMap.builder();
        Map<Chunk, Set<List<DataPage>>> dataToWrite = ImmutableMap.of(new Chunk(0L), createDataPageList(0L, numSlices));
        AsyncRequestBody testBody = fromChunks(
                "test",
                dataToWrite.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> toChunkDataLease(e.getValue()))),
                100,
                spooledChunkMap);
        MockSubscriber subscriber = new MockSubscriber(consumerCallLimit, true);
        testBody.subscribe(subscriber);
        assertTrue(subscriber.isOnCompleteCalled());
        assertEquals(numSlices + 1, subscriber.getOnNextCounter());
        // This has an extra call to request because of nested calls to request() from onNext()
        assertEquals(numSlices + 2, subscriber.getRequestCounter());
        verifyExpectedOutput(dataToWrite, spooledChunkMap.buildOrThrow(), subscriber.getWrittenRawData());
    }

    @Test
    public void testMultiChunk()
    {
        testMultiChunk(false);
    }

    @Test
    public void testMultiChunkNested()
    {
        testMultiChunk(true);
    }

    private void testMultiChunk(boolean nestedRequests)
    {
        final int chunk0NumSlices = 2;
        final int chunk1NumSlices = 3;
        final int chunk2NumSlices = 1;
        final int consumerCallLimit = 2;
        // Use sorted maps to ensure that verifier has dataToWrite (chunk key) and spooledChunkMap (long key) in the same order.
        // This is needed because the ordering with HashMaps will not be the same when the key types are different.
        ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = new ImmutableSortedMap.Builder<>(Ordering.natural());
        Map<Chunk, Set<List<DataPage>>> dataToWrite = createChunkToSliceMap(List.of(chunk0NumSlices, chunk1NumSlices, chunk2NumSlices));
        AsyncRequestBody testBody = fromChunks(
                "test",
                createChunkToLeaseMap(dataToWrite),
                100,
                spooledChunkMap);
        MockSubscriber subscriber = new MockSubscriber(consumerCallLimit, nestedRequests);
        testBody.subscribe(subscriber);
        assertTrue(subscriber.isOnCompleteCalled());
        assertEquals(chunk0NumSlices + chunk1NumSlices + chunk2NumSlices + 3, subscriber.getOnNextCounter());
        assertEquals((chunk0NumSlices + chunk1NumSlices + chunk2NumSlices + 3 + consumerCallLimit - 1) / consumerCallLimit, subscriber.getRequestCounter());
        verifyExpectedOutput(dataToWrite, spooledChunkMap.buildOrThrow(), subscriber.getWrittenRawData());
    }

    @Test
    public void testChunkAcrossConsumerCallLimitBoundaryChunk()
    {
        testChunkAcrossConsumerCallLimitBoundary(false);
    }

    @Test
    public void testChunkAcrossConsumerCallLimitBoundaryNested()
    {
        testChunkAcrossConsumerCallLimitBoundary(true);
    }

    private void testChunkAcrossConsumerCallLimitBoundary(boolean nestedRequests)
    {
        final int chunk0NumSlices = 2;
        final int chunk1NumSlices = 1;
        final int consumerCallLimit = 2;
        // Use sorted maps to ensure that verifier has dataToWrite (chunk key) and spooledChunkMap (long key) in the same order.
        // This is needed because the ordering with HashMaps will not be the same when the key types are different.
        ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = new ImmutableSortedMap.Builder<>(Ordering.natural());
        Map<Chunk, Set<List<DataPage>>> dataToWrite = createChunkToSliceMap(List.of(chunk0NumSlices, chunk1NumSlices));
        AsyncRequestBody testBody = fromChunks(
                "test",
                createChunkToLeaseMap(dataToWrite),
                100,
                spooledChunkMap);
        MockSubscriber subscriber = new MockSubscriber(consumerCallLimit, nestedRequests);
        testBody.subscribe(subscriber);
        assertTrue(subscriber.isOnCompleteCalled());
        assertEquals(chunk0NumSlices + chunk1NumSlices + 2, subscriber.getOnNextCounter());
        assertEquals((chunk0NumSlices + chunk1NumSlices + 2 + consumerCallLimit - 1) / consumerCallLimit, subscriber.getRequestCounter());
        verifyExpectedOutput(dataToWrite, spooledChunkMap.buildOrThrow(), subscriber.getWrittenRawData());
    }

    @Test
    public void testChunkOnConsumerCallLimitBoundary()
    {
        testChunkOnConsumerCallLimitBoundary(false);
    }

    @Test
    public void testChunkOnConsumerCallLimitBoundaryNested()
    {
        testChunkOnConsumerCallLimitBoundary(true);
    }

    private void testChunkOnConsumerCallLimitBoundary(boolean nestedRequests)
    {
        final int chunk0NumSlices = 1;
        final int chunk1NumSlices = 2;
        final int consumerCallLimit = 2;
        // Use sorted maps to ensure that verifier has dataToWrite (chunk key) and spooledChunkMap (long key) in the same order.
        // This is needed because the ordering with HashMaps will not be the same when the key types are different.
        ImmutableMap.Builder<Long, SpooledChunk> spooledChunkMap = new ImmutableSortedMap.Builder<>(Ordering.natural());
        Map<Chunk, Set<List<DataPage>>> dataToWrite = createChunkToSliceMap(List.of(chunk0NumSlices, chunk1NumSlices));
        AsyncRequestBody testBody = fromChunks(
                "test",
                createChunkToLeaseMap(dataToWrite),
                100,
                spooledChunkMap);
        MockSubscriber subscriber = new MockSubscriber(consumerCallLimit, nestedRequests);
        testBody.subscribe(subscriber);
        assertTrue(subscriber.isOnCompleteCalled());
        assertEquals(chunk0NumSlices + chunk1NumSlices + 2, subscriber.getOnNextCounter());
        assertEquals((chunk0NumSlices + chunk1NumSlices + 2 + consumerCallLimit - 1) / consumerCallLimit, subscriber.getRequestCounter());
        verifyExpectedOutput(dataToWrite, spooledChunkMap.buildOrThrow(), subscriber.getWrittenRawData());
    }
}
