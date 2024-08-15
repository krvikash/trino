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
package io.trino.plugin.warp.dispatcher.query.classifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Singleton
public class ClassifierFactory
{
    private final StorageEngineConstants storageEngineConstants;
    private final PredicatesCacheService predicatesCacheService;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final MatchCollectIdService matchCollectIdService;
    private final GlobalConfig globalConfig;
    private final NativeConfig nativeConfig;
    private final BufferAllocator bufferAllocator;
    private ImmutableMap<ClassificationType, List<Classifier>> classificationTypeToClassifiers;

    @Inject
    public ClassifierFactory(StorageEngineConstants storageEngineConstants,
            PredicatesCacheService predicatesCacheService,
            BufferAllocator bufferAllocator,
            NativeConfig nativeConfig,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            MatchCollectIdService matchCollectIdService,
            GlobalConfig globalConfig)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.predicatesCacheService = requireNonNull(predicatesCacheService);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.nativeConfig = requireNonNull(nativeConfig);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.matchCollectIdService = requireNonNull(matchCollectIdService);
        this.globalConfig = requireNonNull(globalConfig);
    }

    private void buildClassifiers()
    {
        ImmutableMap.Builder<ClassificationType, List<Classifier>> classificationTypeToClassifiersBuilder = ImmutableMap.builder();
        MatchClassifier matchClassifier = getMatchClassifier();
        PrefilledCollectClassifier prefilledCollectClassifier = new PrefilledCollectClassifier(
                dispatcherProxiedConnectorTransformer,
                globalConfig);
        NativeCollectClassifier nativeCollectClassifier = new NativeCollectClassifier(
                storageEngineConstants.getMatchCollectBufferSize(),
                storageEngineConstants.getMaxChunksInRange(),
                nativeConfig.getBundleSize() - storageEngineConstants.getBundleNonCollectSize(),
                nativeConfig.getCollectTxSize(),
                storageEngineConstants.getMatchTxSize(),
                storageEngineConstants.getMaxMatchColumns(),
                bufferAllocator,
                dispatcherProxiedConnectorTransformer);
        MatchPrepareAfterCollectClassifier matchPrepareAfterCollectClassifier = new MatchPrepareAfterCollectClassifier(matchCollectIdService,
                storageEngineConstants.getMaxMatchColumns());
        PredicateBufferClassifier predicateBufferClassifier = new PredicateBufferClassifier(predicatesCacheService, globalConfig);
        AllProxyDecisionClassifier allProxyDecisionClassifier = new AllProxyDecisionClassifier(dispatcherProxiedConnectorTransformer);
        List<Classifier> classifiers = List.of(
                matchClassifier,
                prefilledCollectClassifier,
                nativeCollectClassifier,
                matchPrepareAfterCollectClassifier,
                predicateBufferClassifier,
                allProxyDecisionClassifier);
        classificationTypeToClassifiersBuilder.put(ClassificationType.QUERY, classifiers);
        classificationTypeToClassifiersBuilder.put(ClassificationType.CHOOSING_ALTERNATIVE, classifiers);
        // these classifiers used by warming flow to decide what to warm.
        // must make sure that any classifier that allocate resources should not be part of this list (E.g. predicateBufferClassifier)
        List<Classifier> warmingClassifiers = List.of(
                matchClassifier,
                prefilledCollectClassifier,
                nativeCollectClassifier,
                matchPrepareAfterCollectClassifier,
                allProxyDecisionClassifier);
        classificationTypeToClassifiersBuilder.put(ClassificationType.WARMING, warmingClassifiers);
        classificationTypeToClassifiersBuilder.put(ClassificationType.CACHE, List.of(nativeCollectClassifier));
        this.classificationTypeToClassifiers = classificationTypeToClassifiersBuilder.buildOrThrow();
    }

    private MatchClassifier getMatchClassifier()
    {
        ImmutableList.Builder<Matcher> matchers = ImmutableList.builder();
        if (globalConfig.getEnableRangeFilter()) {
            matchers.add(new RangeMatcher(globalConfig));
        }
        matchers.add(
                new LuceneElementsMatcher(dispatcherProxiedConnectorTransformer),
                new BasicMatcher());
        return new MatchClassifier(matchers.build(), globalConfig);
    }

    List<Classifier> getClassifiers(ClassificationType classificationType)
    {
        if (classificationTypeToClassifiers == null) {
            buildClassifiers();
        }
        return classificationTypeToClassifiers.get(classificationType);
    }
}
