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
package io.trino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.Distribution;
import io.airlift.stats.TimeStat;
import io.trino.memory.LocalMemoryManager;
import io.trino.memory.MemoryPool;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.plugin.memory.MemoryCacheManagerFactory;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.subquery.cache.MemoryAllocator;
import io.trino.spi.subquery.cache.SubqueryCacheManager;
import io.trino.spi.subquery.cache.SubqueryCacheManagerContext;
import io.trino.spi.subquery.cache.SubqueryCacheManagerFactory;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.SUBQUERY_CACHE_MANAGER_NOT_CONFIGURED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * {@link SubqueryCacheManagerRegistry} is responsible for instantiation of {@link SubqueryCacheManager}.
 * Additionally {@link SubqueryCacheManagerRegistry} manages revoking of {@link SubqueryCacheManager}
 * memory whenever necessary.
 */
public class SubqueryCacheManagerRegistry
{
    private static final Logger log = Logger.get(SubqueryCacheManagerRegistry.class);

    static final File CONFIG_FILE = new File("etc/subquery-cache-manager.properties");
    private static final String SUBQUERY_CACHE_MANAGER_NAME_PROPERTY = "subquery-cache-manager.name";

    private final MemoryPool memoryPool;
    private final boolean enabled;
    private final double revokingThreshold;
    private final double revokingTarget;
    private final Map<String, SubqueryCacheManagerFactory> subqueryCacheManagerFactories = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private final BlockEncodingSerde blockEncodingSerde;
    private final AtomicBoolean revokeRequested = new AtomicBoolean();
    private final Distribution sizeOfRevokedMemoryDistribution = new Distribution();
    private final AtomicInteger nonEmptyRevokeCount = new AtomicInteger();
    private final CacheStats cacheStats;
    private final InternalNode currentNode;
    private final InternalNodeManager internalNodeManager;

    private volatile SubqueryCacheManager subqueryCacheManager;
    private volatile LocalMemoryContext revocableMemoryContext;

    @Inject
    public SubqueryCacheManagerRegistry(CacheConfig cacheConfig, LocalMemoryManager localMemoryManager, BlockEncodingSerde blockEncodingSerde, CacheStats cacheStats, InternalNode currentNode, InternalNodeManager internalNodeManager)
    {
        this(cacheConfig, localMemoryManager, newSingleThreadExecutor(daemonThreadsNamed("subquery-cache-manager-registry")), blockEncodingSerde, cacheStats, currentNode, internalNodeManager);
    }

    @VisibleForTesting
    SubqueryCacheManagerRegistry(
            CacheConfig cacheConfig,
            LocalMemoryManager localMemoryManager,
            ExecutorService executor,
            BlockEncodingSerde blockEncodingSerde,
            CacheStats cacheStats,
            InternalNode currentNode,
            InternalNodeManager internalNodeManager)
    {
        requireNonNull(cacheConfig, "cacheConfig is null");
        requireNonNull(localMemoryManager, "localMemoryManager is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.enabled = cacheConfig.isEnabled();
        this.revokingThreshold = cacheConfig.getRevokingThreshold();
        this.revokingTarget = cacheConfig.getRevokingTarget();
        this.memoryPool = localMemoryManager.getMemoryPool();
        this.executor = executor;
        this.blockEncodingSerde = blockEncodingSerde;
        this.cacheStats = cacheStats;
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
    }

    public void addSubqueryCacheManagerFactory(SubqueryCacheManagerFactory factory)
    {
        requireNonNull(factory, "factory is null");
        if (subqueryCacheManagerFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Cache manager factory '%s' is already registered", factory.getName()));
        }
    }

    public void loadSubqueryCacheManager()
    {
        if (!enabled) {
            // don't load SubqueryCacheManager when caching is not enabled
            return;
        }

        if (!CONFIG_FILE.exists()) {
            // use MemoryCacheManager by default
            loadSubqueryCacheManager(new MemoryCacheManagerFactory(), ImmutableMap.of());
            return;
        }

        Map<String, String> properties = loadProperties(CONFIG_FILE);
        String name = properties.remove(SUBQUERY_CACHE_MANAGER_NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name), "Cache manager configuration %s does not contain %s", CONFIG_FILE, SUBQUERY_CACHE_MANAGER_NAME_PROPERTY);
        loadSubqueryCacheManager(name, properties);
    }

    public synchronized void loadSubqueryCacheManager(String name, Map<String, String> properties)
    {
        SubqueryCacheManagerFactory factory = subqueryCacheManagerFactories.get(name);
        checkArgument(factory != null, "Cache manager factory '%s' is not registered. Available factories: %s", name, subqueryCacheManagerFactories.keySet());
        loadSubqueryCacheManager(factory, properties);
    }

    public synchronized void loadSubqueryCacheManager(SubqueryCacheManagerFactory factory, Map<String, String> properties)
    {
        requireNonNull(factory, "subqueryCacheManagerFactory is null");
        log.info("-- Loading cache manager %s --", factory.getName());

        checkState(subqueryCacheManager == null, "subqueryCacheManager is already loaded");

        revocableMemoryContext = newRootAggregatedMemoryContext(
                createReservationHandler(bytes -> {
                    // do not allocate more memory if it would exceed revoking threshold
                    if (memoryRevokingNeeded(bytes)) {
                        // schedule memory revoke to free up some space for new splits to be cached
                        scheduleMemoryRevoke();
                        return false;
                    }

                    return memoryPool.tryReserveRevocable(bytes);
                }, memoryPool::freeRevocable),
                0)
                .newLocalMemoryContext("SubqueryCacheManager");
        SubqueryCacheManagerContext context = new SubqueryCacheManagerContext()
        {
            @Override
            public MemoryAllocator revocableMemoryAllocator()
            {
                return revocableMemoryContext::trySetBytes;
            }

            @Override
            public BlockEncodingSerde blockEncodingSerde()
            {
                return blockEncodingSerde;
            }

            @Override
            public NodeManager getNodeManager()
            {
                return new NodeManager()
                {
                    @Override
                    public Set<Node> getAllNodes()
                    {
                        return internalNodeManager.getAllNodes().activeNodes().stream().map(node -> (Node) node).collect(Collectors.toSet());
                    }

                    @Override
                    public Set<Node> getWorkerNodes()
                    {
                        return this.getAllNodes().stream().filter(node -> !node.isCoordinator()).collect(Collectors.toSet());
                    }

                    @Override
                    public Node getCurrentNode()
                    {
                        return currentNode;
                    }
                };
            }

            @Override
            public Node getCurrentNode()
            {
                return currentNode;
            }
        };
        SubqueryCacheManager subqueryCacheManager;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            subqueryCacheManager = factory.create(properties, context);
        }
        this.subqueryCacheManager = subqueryCacheManager;

        // revoke cache memory when revoking target is reached
        memoryPool.addListener(_ -> {
            if (memoryRevokingNeeded(0)) {
                scheduleMemoryRevoke();
            }
        });

        log.info("-- Loaded cache manager %s --", factory.getName());
    }

    public SubqueryCacheManager getSubqueryCacheManager()
    {
        SubqueryCacheManager subqueryCacheManager = this.subqueryCacheManager;
        if (subqueryCacheManager == null) {
            throw new TrinoException(SUBQUERY_CACHE_MANAGER_NOT_CONFIGURED, "Cache manager must be configured for cache capabilities to be fully functional");
        }
        return subqueryCacheManager;
    }

    public void flushCache()
    {
        getFutureValue(executor.submit(() -> {
            long bytesToRevoke = memoryPool.getMaxBytes() - memoryPool.getFreeBytes();
            if (bytesToRevoke > 0) {
                subqueryCacheManager.revokeMemory(bytesToRevoke);
            }
        }));
    }

    @Managed
    public long getRevocableBytes()
    {
        if (revocableMemoryContext == null) {
            return 0;
        }
        return revocableMemoryContext.getBytes();
    }

    @Managed
    @Nested
    public Distribution getDistributionSizeRevokedMemory()
    {
        return sizeOfRevokedMemoryDistribution;
    }

    @Managed
    public int getNonEmptyRevokeCount()
    {
        return nonEmptyRevokeCount.get();
    }

    private static Map<String, String> loadProperties(File configFile)
    {
        try {
            return new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }
    }

    private void scheduleMemoryRevoke()
    {
        // allow at most one revoke request to be scheduled
        if (revokeRequested.getAndSet(true)) {
            return;
        }
        executor.submit(() -> {
            revokeRequested.set(false);
            do {
                if (!revokeMemory()) {
                    return;
                }
            }
            while (memoryRevokingNeeded(0));
        });
    }

    private boolean revokeMemory()
    {
        long bytesToRevoke = -memoryPool.getFreeBytes() + (long) (memoryPool.getMaxBytes() * (1.0 - revokingTarget));
        if (bytesToRevoke <= 0) {
            return false;
        }

        long revokedBytes;
        try (TimeStat.BlockTimer ignore = cacheStats.recordRevokeMemoryTime()) {
            revokedBytes = subqueryCacheManager.revokeMemory(bytesToRevoke);
        }

        if (revokedBytes > 0) {
            sizeOfRevokedMemoryDistribution.add(revokedBytes);
            nonEmptyRevokeCount.incrementAndGet();
            return true;
        }

        return false;
    }

    private boolean memoryRevokingNeeded(long additionalRevocableBytes)
    {
        return memoryPool.getFreeBytes() - additionalRevocableBytes < memoryPool.getMaxBytes() * (1.0 - revokingThreshold);
    }

    private static MemoryReservationHandler createReservationHandler(Function<Long, Boolean> tryReserveHandler, Consumer<Long> freeHandler)
    {
        return new MemoryReservationHandler()
        {
            @Override
            public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
            {
                throw new IllegalStateException();
            }

            @Override
            public boolean tryReserveMemory(String allocationTag, long delta)
            {
                if (delta >= 0) {
                    return tryReserveHandler.apply(delta);
                }

                freeHandler.accept(-delta);
                return true;
            }
        };
    }
}
