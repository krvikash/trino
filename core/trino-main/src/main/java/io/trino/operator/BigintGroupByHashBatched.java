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
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BigintType;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.GroupByHash.createBigintGroupByHash;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

/**
 * Optimized version of {@link BigintGroupByHash} that uses batching to amortize the cost of
 * random memory reads, and keeps the group ids together with values in the same hash table, to minimize the
 * number of the random reads.
 */
public class BigintGroupByHashBatched
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(BigintGroupByHashBatched.class);
    // Smaller batch size than in BigintGroupByHash to increase the chance that batch will fit into CPU L1 cache.
    private static final int BATCH_SIZE = 64;
    public static final int ENTRY_SIZE = Integer.BYTES + Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);

    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;

    private final int maxGroupCount;
    // reusable array for computing hash batches into
    private final int[] currentHashes = new int[BATCH_SIZE];
    private final int[] initialGroupIds = new int[BATCH_SIZE];

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // The hash table with int groupId and long value
    // Ideally, we should use MemorySegment but the benchmarks show significant
    // performance degradation that nullifies the improvement over the original implementation.
    private byte[] hashTable;

    // groupId for the null value
    private int nullGroupId = EMPTY_SLOT;

    // reverse index from the groupId back to the value
    private long[] valuesByGroupId;

    private int nextGroupId;
    private DictionaryLookBack dictionaryLookBack;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;

    public BigintGroupByHashBatched(int expectedSize, UpdateMemory updateMemory)
    {
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        maxGroupCount = calculateMaxFill(Integer.MAX_VALUE / ENTRY_SIZE);
        hashTable = new byte[hashCapacity * ENTRY_SIZE];
        markEmptySlots(hashTable);

        valuesByGroupId = new long[maxFill];

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    private BigintGroupByHashBatched(BigintGroupByHashBatched other)
    {
        maxGroupCount = other.maxGroupCount;
        hashCapacity = other.hashCapacity;
        maxFill = other.maxFill;
        mask = other.mask;
        hashTable = Arrays.copyOf(other.hashTable, other.hashTable.length);
        nullGroupId = other.nullGroupId;
        valuesByGroupId = Arrays.copyOf(other.valuesByGroupId, maxFill);
        nextGroupId = other.nextGroupId;
        dictionaryLookBack = other.dictionaryLookBack == null ? null : other.dictionaryLookBack.copy();
        updateMemory = other.updateMemory;
        preallocatedMemoryInBytes = other.preallocatedMemoryInBytes;
        currentPageSizeInBytes = other.currentPageSizeInBytes;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(hashTable) +
                sizeOf(valuesByGroupId) +
                sizeOf(currentHashes) +
                sizeOf(initialGroupIds) +
                preallocatedMemoryInBytes;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void startReleasingOutput()
    {
        dictionaryLookBack = null;
        currentPageSizeInBytes = 0;
        hashTable = null;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, valuesByGroupId[groupId]);
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(0);
        if (block instanceof RunLengthEncodedBlock rleBlock) {
            return new AddRunLengthEncodedPageWork(rleBlock);
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            return new AddDictionaryPageWork(dictionaryBlock);
        }

        return new AddPageWork(block);
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(0);
        if (block instanceof RunLengthEncodedBlock rleBlock) {
            return new GetRunLengthEncodedGroupIdsWork(rleBlock);
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            return new GetDictionaryGroupIdsWork(dictionaryBlock);
        }

        return new GetGroupIdsWork(block);
    }

    @Override
    public long getRawHash(int groupId)
    {
        return BigintType.hash(valuesByGroupId[groupId]);
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    @Override
    public GroupByHash copy()
    {
        return new BigintGroupByHashBatched(this);
    }

    private void putIfAbsent(int initialPosition, int batchSize, Block block, int[] outGroupIds, int[] hashes, int[] initialGroupIds)
    {
        for (int i = 0; i < batchSize; i++) {
            if (!block.isNull(initialPosition + i)) {
                initialGroupIds[i] = (int) INT_HANDLE.get(hashTable, hashes[i]);
            }
        }
        for (int i = 0; i < batchSize; i++) {
            int position = initialPosition + i;
            // output the group id for this row
            outGroupIds[position] = putIfAbsent(position, block, hashes[i], initialGroupIds[i]);
        }
    }

    private void putIfAbsent(int initialPosition, int batchSize, Block block, int[] hashes, int[] initialGroupIds)
    {
        for (int i = 0; i < batchSize; i++) {
            if (!block.isNull(initialPosition + i)) {
                initialGroupIds[i] = (int) INT_HANDLE.get(hashTable, hashes[i]);
            }
        }
        for (int i = 0; i < batchSize; i++) {
            int position = initialPosition + i;
            putIfAbsent(position, block, hashes[i], initialGroupIds[i]);
        }
    }

    private int putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (nullGroupId == EMPTY_SLOT) {
                // set null group id
                nullGroupId = nextGroupId++;
            }

            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        int hashPosition = getHashPosition(value, mask);

        return putValueIfAbsent(value, hashPosition, -2);
    }

    private int putIfAbsent(int position, Block block, int hashPosition, int initialGroupId)
    {
        if (block.isNull(position)) {
            if (nullGroupId == EMPTY_SLOT) {
                // set null group id
                nullGroupId = nextGroupId++;
            }

            return nullGroupId;
        }

        return putValueIfAbsent(BIGINT.getLong(block, position), hashPosition, initialGroupId);
    }

    private int putValueIfAbsent(long value, int hashPosition, int initialGroupId)
    {
        if (initialGroupId >= 0 && value == (long) LONG_HANDLE.get(hashTable, hashPosition + Integer.BYTES)) {
            return initialGroupId;
        }

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = (int) INT_HANDLE.get(hashTable, hashPosition);
            if (groupId == EMPTY_SLOT) {
                break;
            }

            if (value == (long) LONG_HANDLE.get(hashTable, hashPosition + Integer.BYTES)) {
                return groupId;
            }

            hashPosition = getNextHashPosition(hashPosition, hashTable);
        }

        return addNewGroup(hashPosition, value);
    }

    private void computeHashes(Block block, int offset, int batchSize, int[] hashes)
    {
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < batchSize; i++) {
            int position = offset + i;
            if (!mayHaveNull || !block.isNull(position)) {
                hashes[i] = getHashPosition(BIGINT.getLong(block, position), mask);
            }
        }
    }

    private int addNewGroup(int hashPosition, long value)
    {
        // record group id in hash
        int groupId = nextGroupId++;

        INT_HANDLE.set(hashTable, hashPosition, groupId);
        LONG_HANDLE.set(hashTable, hashPosition + Integer.BYTES, value);
        valuesByGroupId[groupId] = value;

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
    }

    private boolean tryRehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > maxGroupCount) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed %s entries".formatted(maxGroupCount));
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for hashTable containing group ids and values, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = newCapacity * (long) (Long.BYTES + Integer.BYTES) + ((long) calculateMaxFill(newCapacity)) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }

        int newMask = newCapacity - 1;
        byte[] newHashTable = new byte[newCapacity * ENTRY_SIZE];
        markEmptySlots(newHashTable);

        for (int i = 0; i < hashTable.length; i += ENTRY_SIZE) {
            int groupId = (int) INT_HANDLE.get(hashTable, i);

            if (groupId != EMPTY_SLOT) {
                long value = (long) LONG_HANDLE.get(hashTable, i + Integer.BYTES);
                int hashPosition = getHashPosition(value, newMask);

                // find an empty slot for the address
                while ((int) INT_HANDLE.get(newHashTable, hashPosition) != EMPTY_SLOT) {
                    hashPosition = getNextHashPosition(hashPosition, newHashTable);
                }

                // record the mapping
                INT_HANDLE.set(newHashTable, hashPosition, groupId);
                LONG_HANDLE.set(newHashTable, hashPosition + Integer.BYTES, value);
            }
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        hashTable = newHashTable;

        this.valuesByGroupId = Arrays.copyOf(valuesByGroupId, maxFill);

        preallocatedMemoryInBytes = 0;
        // release temporary memory reservation
        updateMemory.update();
        return true;
    }

    private static void markEmptySlots(byte[] hashTable)
    {
        for (int i = 0; i < hashTable.length; i += ENTRY_SIZE) {
            INT_HANDLE.set(hashTable, i, EMPTY_SLOT);
        }
    }

    private static int getNextHashPosition(int hashPosition, byte[] hashTable)
    {
        hashPosition = hashPosition + ENTRY_SIZE;
        if (hashPosition >= hashTable.length) {
            hashPosition = 0;
        }
        return hashPosition;
    }

    private boolean needRehash()
    {
        return nextGroupId >= maxFill;
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        return ((int) (murmurHash3(rawHash) & mask)) * ENTRY_SIZE;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private void updateDictionaryLookBack(Block dictionary)
    {
        if (dictionaryLookBack == null || dictionaryLookBack.getDictionary() != dictionary) {
            dictionaryLookBack = new DictionaryLookBack(dictionary);
        }
    }

    private int registerGroupId(Block dictionary, int positionInDictionary)
    {
        if (dictionaryLookBack.isProcessed(positionInDictionary)) {
            return dictionaryLookBack.getGroupId(positionInDictionary);
        }

        int groupId = putIfAbsent(positionInDictionary, dictionary);
        dictionaryLookBack.setProcessed(positionInDictionary, groupId);
        return groupId;
    }

    public boolean shouldFallback(int newPositions)
    {
        return nextGroupId + newPositions > maxGroupCount;
    }

    public Optional<GroupByHash> fallbackToBigintGroupByHash()
    {
        // An estimate of how much extra memory is needed before we can go ahead and create a new BigintGroupByHash.
        // This includes the capacity for values and groupIds
        preallocatedMemoryInBytes = hashCapacity * (long) (Long.BYTES + Integer.BYTES);
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return Optional.empty();
        }
        long[] values = new long[hashCapacity];
        int[] groupIds = new int[hashCapacity];
        int entryIndex = 0;
        for (int i = 0; i < hashTable.length; i += ENTRY_SIZE) {
            groupIds[entryIndex] = (int) INT_HANDLE.get(hashTable, i);
            values[entryIndex] = (long) LONG_HANDLE.get(hashTable, i + 4);
            entryIndex++;
        }
        // free memory from the current hash table
        hashTable = new byte[0];
        GroupByHash bigintGroupByHash = createBigintGroupByHash(updateMemory, nextGroupId, nullGroupId, valuesByGroupId, values, groupIds);
        preallocatedMemoryInBytes = 0;
        // release temporary memory reservation
        updateMemory.update();
        return Optional.of(bigintGroupByHash);
    }

    @VisibleForTesting
    class AddPageWork
            implements Work<Void>
    {
        private final Block block;

        private int lastPosition;

        public AddPageWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, currentHashes.length);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                computeHashes(block, lastPosition, batchSize, currentHashes);
                putIfAbsent(lastPosition, batchSize, block, currentHashes, initialGroupIds);

                lastPosition += batchSize;
                remainingPositions -= batchSize;
            }
            verify(lastPosition == positionCount);
            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class AddDictionaryPageWork
            implements Work<Void>
    {
        private final Block dictionary;
        private final DictionaryBlock block;

        private int lastPosition;

        public AddDictionaryPageWork(DictionaryBlock block)
        {
            this.block = requireNonNull(block, "block is null");
            this.dictionary = block.getDictionary();
            updateDictionaryLookBack(dictionary);
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = block.getId(lastPosition);
                registerGroupId(dictionary, positionInDictionary);
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class AddRunLengthEncodedPageWork
            implements Work<Void>
    {
        private final RunLengthEncodedBlock block;

        private boolean finished;

        public AddRunLengthEncodedPageWork(RunLengthEncodedBlock block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            checkState(!finished);
            if (block.getPositionCount() == 0) {
                finished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            putIfAbsent(0, block.getValue());
            finished = true;

            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    @VisibleForTesting
    class GetGroupIdsWork
            implements Work<int[]>
    {
        private final int[] groupIds;
        private final Block block;

        private boolean finished;
        private int lastPosition;

        public GetGroupIdsWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
            this.groupIds = new int[block.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            int remainingPositions = positionCount - lastPosition;

            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, currentHashes.length);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                computeHashes(block, lastPosition, batchSize, currentHashes);
                putIfAbsent(lastPosition, batchSize, block, groupIds, currentHashes, initialGroupIds);

                lastPosition += batchSize;
                remainingPositions -= batchSize;
            }
            verify(lastPosition == positionCount);
            return true;
        }

        @Override
        public int[] getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetDictionaryGroupIdsWork
            implements Work<int[]>
    {
        private final int[] groupIds;
        private final Block dictionary;
        private final DictionaryBlock block;

        private boolean finished;
        private int lastPosition;

        public GetDictionaryGroupIdsWork(DictionaryBlock block)
        {
            this.block = requireNonNull(block, "block is null");
            this.dictionary = block.getDictionary();
            updateDictionaryLookBack(dictionary);

            this.groupIds = new int[block.getPositionCount()];
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            checkState(!finished);

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // putIfAbsent will rehash automatically if rehash is needed, unless there isn't enough memory to do so.
            // Therefore needRehash will not generally return true even if we have just crossed the capacity boundary.
            while (lastPosition < positionCount && !needRehash()) {
                int positionInDictionary = block.getId(lastPosition);
                int groupId = registerGroupId(dictionary, positionInDictionary);
                groupIds[lastPosition] = groupId;
                lastPosition++;
            }
            return lastPosition == positionCount;
        }

        @Override
        public int[] getResult()
        {
            checkState(lastPosition == block.getPositionCount(), "process has not yet finished");
            checkState(!finished, "result has produced");
            finished = true;
            return groupIds;
        }
    }

    @VisibleForTesting
    class GetRunLengthEncodedGroupIdsWork
            implements Work<int[]>
    {
        private final RunLengthEncodedBlock block;

        int groupId = EMPTY_SLOT;
        private boolean processFinished;
        private boolean resultProduced;

        public GetRunLengthEncodedGroupIdsWork(RunLengthEncodedBlock block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            checkState(!processFinished);
            if (block.getPositionCount() == 0) {
                processFinished = true;
                return true;
            }

            // needRehash() == false indicates we have reached capacity boundary and a rehash is needed.
            // We can only proceed if tryRehash() successfully did a rehash.
            if (needRehash() && !tryRehash()) {
                return false;
            }

            // Only needs to process the first row since it is Run Length Encoded
            groupId = putIfAbsent(0, block.getValue());
            processFinished = true;
            return true;
        }

        @Override
        public int[] getResult()
        {
            checkState(processFinished);
            checkState(!resultProduced);
            resultProduced = true;

            int[] result = new int[block.getPositionCount()];
            Arrays.fill(result, groupId);
            return result;
        }
    }

    private boolean ensureHashTableSize(int batchSize)
    {
        int positionCountUntilRehash = maxFill - nextGroupId;
        while (positionCountUntilRehash < batchSize) {
            if (!tryRehash()) {
                return false;
            }
            positionCountUntilRehash = maxFill - nextGroupId;
        }
        return true;
    }

    static final class DictionaryLookBack
    {
        private final Block dictionary;
        private final int[] processed;

        public DictionaryLookBack(Block dictionary)
        {
            this.dictionary = dictionary;
            this.processed = new int[dictionary.getPositionCount()];
            Arrays.fill(processed, EMPTY_SLOT);
        }

        private DictionaryLookBack(DictionaryLookBack other)
        {
            this.dictionary = other.dictionary;
            this.processed = Arrays.copyOf(other.processed, other.processed.length);
        }

        public Block getDictionary()
        {
            return dictionary;
        }

        public int getGroupId(int position)
        {
            return processed[position];
        }

        public boolean isProcessed(int position)
        {
            return processed[position] != EMPTY_SLOT;
        }

        public void setProcessed(int position, int groupId)
        {
            processed[position] = groupId;
        }

        public DictionaryLookBack copy()
        {
            return new DictionaryLookBack(this);
        }
    }
}
