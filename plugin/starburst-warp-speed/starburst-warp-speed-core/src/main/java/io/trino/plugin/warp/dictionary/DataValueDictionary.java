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
package io.trino.plugin.warp.dictionary;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.dispatcher.model.DictionaryKey;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.spi.block.Block;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataValueDictionary
        implements WriteDictionary, ReadDictionary
{
    static final int MAX_DICTIONARY_SIZE = 1024 * 1024; // Note: the actual dictionary size in double, each element we add to preBlock
    private static final Logger logger = Logger.get(DataValueDictionary.class);

    private static final ArrayList<Short> indicesValue = new ArrayList<>(DictionaryConfig.DICTIONARY_MAX_SIZE);

    static {
        for (int i = 0; i < DictionaryConfig.DICTIONARY_MAX_SIZE; i++) {
            indicesValue.add((short) i);
        }
    }

    private final DictionaryKey dictionaryKey;
    private final Lock addKeyLock;
    private final ConcurrentHashMap<Object, Short> writeDictionary;
    private final int fixedRecTypeLength;
    private final DictionaryStats dictionaryStats;
    private final AtomicInteger usingTransactions = new AtomicInteger();
    private final int dictionaryMaxSize;
    private final int maxDictionaryCacheWeight;

    private int dictionaryWeight;
    private int attachedDictionarySize;
    private int maxRecTypeLength;
    private List<Object> readDictionary;
    private Block preBlock; // optimization. dictionary values as block. Used by Fillers
    private boolean shouldExport;
    private boolean isImmutable;

    /**
     * dictionary index (value) is increment in serial order from zero. in case it over Short.MAX_VALUE we will cast to unsigned short value.
     * it's done because we need to use ShortBuffer, which gets only short values.
     * in Fillers, we convert back index value to positive number with Short.toUnsignedInt function.
     */
    DataValueDictionary(
            DictionaryConfig dictionaryConfig,
            DictionaryKey dictionaryKey,
            int fixedRecTypeLength,
            int maxRecTypeLength,
            DictionaryStats dictionaryStats)
    {
        this.dictionaryKey = dictionaryKey;
        this.addKeyLock = new ReentrantLock();
        this.writeDictionary = new ConcurrentHashMap<>();
        this.dictionaryWeight = 0;
        this.attachedDictionarySize = 0;
        this.readDictionary = new ArrayList<>();
        this.dictionaryMaxSize = dictionaryConfig.getDictionaryMaxSize();
        this.fixedRecTypeLength = fixedRecTypeLength;
        this.maxDictionaryCacheWeight = dictionaryConfig.getMaxDictionaryCacheWeight();
        // maxRecTypeLength
        //   new or loaded fixed length dictionaries: equal to fixedRecTypeLength
        //   new varlen dictionaries: zero (and will be updated on each new key)
        //   loaded varlen dictionaries: the loaded record type length
        this.maxRecTypeLength = maxRecTypeLength;
        this.dictionaryStats = dictionaryStats;
    }

    @Override
    public Short get(Object key)
    {
        Short index = writeDictionary.get(key);
        if (index == null) {
            addKeyLock.lock();
            try {
                Object value = getKeyValue(key);
                index = writeDictionary.computeIfAbsent(value, _ -> {
                    int incDictionaryWeight = addedWeight(value);
                    if (writeDictionary.size() == dictionaryMaxSize || (incDictionaryWeight + dictionaryWeight > maxDictionaryCacheWeight)) {
                        throw new DictionaryException("dictionary get failed due max size", WarmUpElementState.State.FAILED_TEMPORARILY, dictionaryKey, DictionaryState.DICTIONARY_MAX_EXCEPTION);
                    }
                    try {
                        increaseDictionaryWeight(incDictionaryWeight);
                        int newIndex = writeDictionary.size();
                        readDictionary.add(value);
                        return indicesValue.get(newIndex);
                    }
                    catch (Exception e) {
                        logger.error(e, "failed to append key %s to dictionary %s", value, this);
                        throw new RuntimeException(e);
                    }
                });
            }
            finally {
                addKeyLock.unlock();
            }
        }
        return index;
    }

    private Object getKeyValue(Object key)
    {
        if (key instanceof Slice slice && !slice.isCompact()) {
            return slice.copy();
        }
        return key;
    }

    // we assume this API is called under a lock to get a dictionary reference, thus no need to take the key lock
    @Override
    public void loadKey(Object key, int index)
    {
        readDictionary.add(key);
        attachedDictionarySize++;
        writeDictionary.put(key, indicesValue.get(index));
        int incDictionaryWeight = addedWeight(key);
        increaseDictionaryWeight(incDictionaryWeight);
    }

    @Override
    public DictionaryKey getDictionaryKey()
    {
        return dictionaryKey;
    }

    @Override
    public Object get(int index)
    {
        return readDictionary.get(index);
    }

    @Override
    public int getReadSize()
    {
        return attachedDictionarySize;
    }

    @Override
    public int getReadAvailableSize()
    {
        return getWriteSize();
    }

    @Override
    public int getWriteSize()
    {
        addKeyLock.lock();
        try {
            return writeDictionary.size();
        }
        finally {
            addKeyLock.unlock();
        }
    }

    @Override
    public int getRecTypeLength()
    {
        return maxRecTypeLength;
    }

    public void setImmutable()
    {
        isImmutable = true;
    }

    public boolean isImmutable()
    {
        return isImmutable;
    }

    @Override
    public Block getPreBlockDictionaryIfExists(int rowsToFill, DictionaryCacheService dictionaryCacheService, RecTypeCode recTypeCode)
    {
        if (rowsToFill < (getReadSize() / 2)) {
            dictionaryStats.incdictionary_pre_block_not_used();
            return null;
        }
        // extra if statement before the synchronize so we won't lock all if not needed
        if (preBlock != null) {
            dictionaryStats.incdictionary_pre_block_used();
        }
        else {
            synchronized (this) {
                if (preBlock == null) {
                    dictionaryCacheService.loadPreBlock(recTypeCode, this);
                    dictionaryStats.incdictionary_pre_block_created();
                }
            }
        }
        return preBlock;
    }

    // called under synchronized
    public void setDictionaryPreBlock(Block preBlock)
    {
        this.preBlock = preBlock;
    }

    DictionaryToWrite createDictionaryToWrite()
    {
        addKeyLock.lock();
        try {
            return new DictionaryToWrite(readDictionary.toArray(), writeDictionary.size(), maxRecTypeLength, dictionaryWeight);
        }
        finally {
            addKeyLock.unlock();
        }
    }

    // called under synchronized
    void dictionaryAttached()
    {
        preBlock = null;
        shouldExport = true;
    }

    boolean canSkipWrite()
    {
        return getWriteSize() == attachedDictionarySize;
    }

    void reset()
    {
        writeDictionary.clear();
        readDictionary = new ArrayList<>();
        preBlock = null;
        attachedDictionarySize = 0;
    }

    public int getDictionaryWeight()
    {
        return dictionaryWeight;
    }

    public boolean isVarlen()
    {
        return fixedRecTypeLength == 0;
    }

    private void increaseDictionaryWeight(int incDictionaryWeight)
    {
        if (isVarlen()) {
            dictionaryStats.adddictionaries_varlen_str_weight(incDictionaryWeight);
        }
        else {
            incDictionaryWeight = fixedRecTypeLength; // for preDictionary
        }
        dictionaryWeight += incDictionaryWeight;
        dictionaryStats.adddictionaries_weight(incDictionaryWeight);
    }

    private int addedWeight(Object key)
    {
        int incDictionaryWeight;
        if (isVarlen()) {
            maxRecTypeLength = Math.max(((Slice) key).length(), maxRecTypeLength);
            incDictionaryWeight = ((Slice) key).length(); // for preDictionary
        }
        else {
            incDictionaryWeight = fixedRecTypeLength; // for preDictionary
        }
        return incDictionaryWeight;
    }

    public int getUsingTransactions()
    {
        return usingTransactions.get();
    }

    public void incUsingTransactions()
    {
        this.usingTransactions.incrementAndGet();
    }

    public int decUsingTransactions()
    {
        return this.usingTransactions.decrementAndGet();
    }

    @Override
    public String toString()
    {
        return "DataValueDictionary{" +
                "dictionaryKey=" + dictionaryKey +
                ", writeDictionarySize=" + writeDictionary.size() +
                ", fixedRecTypeLength=" + fixedRecTypeLength +
                ", usingTransactions=" + usingTransactions +
                ", dictionaryWeight=" + dictionaryWeight +
                ", attachedDictionarySize=" + attachedDictionarySize +
                ", recTypeLength=" + maxRecTypeLength +
                ", preBlockExist=" + (preBlock != null ? "true" : "false") +
                ", shouldExport=" + shouldExport +
                ", isImmutable=" + isImmutable +
                '}';
    }
}
