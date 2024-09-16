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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.dispatcher.model.DictionaryInfo;
import io.trino.plugin.warp.dispatcher.model.DictionaryKey;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.DictionaryStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Singleton
public class DictionaryCacheService
{
    private final DictionaryStats dictionaryStats;
    public static final String DICTIONARY_STAT_GROUP = "dictionary";
    public static final int MINIMUM_LEN_FOR_DICTIONARY = 4;
    public static final RecTypeCode DICTIONARY_REC_TYPE_CODE = RecTypeCode.REC_TYPE_SMALLINT;
    public static final int DICTIONARY_REC_TYPE_CODE_NUM = DICTIONARY_REC_TYPE_CODE.ordinal();
    public static final int DICTIONARY_REC_TYPE_LENGTH = Short.BYTES;

    private static final Logger logger = Logger.get(DictionaryCacheService.class);

    private final DictionariesCache dictionariesCache;
    private final AttachDictionaryService attachDictionaryService;
    private final DictionaryConfig dictionaryConfig;

    @Inject
    public DictionaryCacheService(DictionaryConfig dictionaryConfig,
            MetricsManager metricsManager,
            AttachDictionaryService attachDictionaryService)
    {
        this.attachDictionaryService = requireNonNull(attachDictionaryService);
        this.dictionaryConfig = dictionaryConfig;
        this.dictionariesCache = new DictionariesCache(dictionaryConfig, metricsManager, attachDictionaryService);
        this.dictionaryStats = requireNonNull(metricsManager).registerMetric(DictionaryStats.create(DICTIONARY_STAT_GROUP));
    }

    public WriteDictionary computeWriteIfAbsent(DictionaryKey dictionaryKey, RecTypeCode recTypeCode)
    {
        return dictionariesCache.getWriteDictionary(dictionaryKey, recTypeCode);
    }

    public ReadDictionary computeReadIfAbsent(DictionaryKey dictionaryKey,
            int usedDictionarySize,
            RecTypeCode recTypeCode,
            int recTypeLength,
            int dictionaryOffset,
            String rowGroupFilePath)
    {
        return dictionariesCache.getReadDictionary(dictionaryKey, usedDictionarySize, recTypeCode, recTypeLength, dictionaryOffset, rowGroupFilePath);
    }

    public long getLastCreatedTimestamp(SchemaTableColumn schemaTableColumn, String nodeIdentifier)
    {
        return dictionariesCache.getLastCreatedTimestamp(schemaTableColumn, nodeIdentifier);
    }

    public void updateOnFailedWrite(DictionaryKey dictionaryKey)
    {
        dictionariesCache.incFailedWriteCount(dictionaryKey);
    }

    public DictionaryState calculateDictionaryStateForWrite(DictionaryKey dictionaryKey, WarmUpElement warmUpElement, Boolean dictionaryEnabled)
    {
        if (!isDictionaryValidForColumn(warmUpElement, dictionaryEnabled)) {
            return DictionaryState.DICTIONARY_NOT_EXIST;
        }
        DictionaryState res;
        DictionaryInfo dictionaryInfo = warmUpElement.getDictionaryInfo();
        if (dictionariesCache.hasExceededFailedWriteLimit(dictionaryKey)) {
            res = DictionaryState.DICTIONARY_REJECTED;
        }
        else if (dictionaryInfo == null) {
            res = DictionaryState.DICTIONARY_VALID;
        }
        else {
            res = dictionaryInfo.dictionaryState();
        }
        return res;
    }

    public int writeDictionary(DictionaryKey dictionaryKey,
            RecTypeCode recTypeCode,
            int dictionaryOffset,
            String rowGroupFilePath)
    {
        DataValueDictionary dataValueDictionary = dictionariesCache.getActiveDataValueDictionary(dictionaryKey);
        if (dataValueDictionary.canSkipWrite()) {
            return 0;
        }

        synchronized (dataValueDictionary) {
            if (dataValueDictionary.canSkipWrite()) {
                return 0;
            }

            try {
                // add keys lock is different from synchronized lock, so we must get a copy of the dictionary before we write it
                DictionaryToWrite dictionaryToWrite = dataValueDictionary.createDictionaryToWrite();
                int dictionarySize = attachDictionaryService.save(dictionaryToWrite, recTypeCode, dictionaryOffset, rowGroupFilePath);
                try {
                    dictionariesCache.loadPreBlock(recTypeCode, dataValueDictionary);
                }
                catch (Exception e) {
                    throw new TrinoException(WarpErrorCode.WARP_DICTIONARY_ERROR, e);
                }
                dataValueDictionary.dictionaryAttached();
                return dictionarySize;
            }
            catch (Exception e) {
                throw new TrinoException(WarpErrorCode.WARP_DICTIONARY_ERROR, e);
            }
        }
    }

    public Map<DebugDictionaryKey, DebugDictionaryMetadata> getWorkerDictionaryMetadata()
    {
        return dictionariesCache.getWriteDictionaryMetadata();
    }

    public void reset()
    {
        dictionariesCache.resetAllDictionaries();
    }

    public int resetMemoryDictionaries(long dictionaryCacheTotalWight, int concurrency)
    {
        return dictionariesCache.resetMemoryDictionaries(dictionaryCacheTotalWight, concurrency);
    }

    private boolean isDictionaryEnabledForRecType(RecTypeCode recTypeCode, Boolean sessionPropertyEnableDictionary)
    {
        if (!recTypeCode.isSupportedDictionary()) {
            return false;
        }
        // if we have a session property than we go by its value
        if (sessionPropertyEnableDictionary != null) {
            return sessionPropertyEnableDictionary;
        }
        // no session property we use the global config list and flag
        boolean inExceptionalList = dictionaryConfig.getExceptionalListDictionary().contains(recTypeCode);
        if (dictionaryConfig.getEnableDictionary()) {
            // dictionary is enabled - the list contains disabled record types
            return !inExceptionalList;
        }
        // dictionary is disabled - the list contains enabled record types
        return inExceptionalList;
    }

    private boolean isDictionaryValidForColumn(WarmUpElement warmUpElement, Boolean sessionPropertyEnableDictionary)
    {
        return warmUpElement.getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA &&
                isDictionaryEnabledForRecType(warmUpElement.getRecTypeCode(), sessionPropertyEnableDictionary) &&
                warmUpElement.getRecTypeLength() >= MINIMUM_LEN_FOR_DICTIONARY;
    }

    public void releaseActiveDictionary(DictionaryKey dictionaryKey)
    {
        try {
            dictionariesCache.releaseDictionary(dictionaryKey);
        }
        catch (Exception e) {
            logger.error(e, "failed to release dictionary %s", dictionaryKey);
        }
    }

    public DictionaryCacheConfig getDictionaryConfig()
    {
        return new DictionaryCacheConfig(
                dictionariesCache.getDictionaryCacheTotalSize(),
                dictionariesCache.getCacheConcurrency(),
                dictionaryConfig.getDictionaryMaxSize());
    }

    public Map<String, Integer> getDictionaryCachedKeys()
    {
        return dictionariesCache.getDictionaryCachedKeys();
    }

    public void releaseActiveDictionaries(List<DictionaryWarmInfo> dictionariesWarmInfos)
    {
        for (DictionaryWarmInfo dictionaryWarmInfo : dictionariesWarmInfos) {
            if (dictionaryWarmInfo.dictionaryState() == DictionaryState.DICTIONARY_REJECTED) {
                dictionaryStats.incdictionary_rejected_elements_count();
            }
            else if (dictionaryWarmInfo.dictionaryState() == DictionaryState.DICTIONARY_VALID) {
                releaseActiveDictionary(dictionaryWarmInfo.dictionaryKey());
                dictionaryStats.incdictionary_success_elements_count();
            }
        }
    }

    public record DictionaryCacheConfig(
            long maxDictionaryTotalCacheWeight,
            int concurrency,
            long dictionaryMaxSize) {}
}
