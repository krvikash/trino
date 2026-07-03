/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.starburst.materialization.ir.Operation;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.spi.connector.CatalogSchemaTableName;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class MaterializationIndex
        implements MaterializationMetastore
{
    private static final Logger log = Logger.get(MaterializationIndex.class);

    private final VersionAwareMaterializationMetastore materializationMetastore;

    // The read path (getMaterializations) does a single volatile read of an immutable Snapshot and never locks.
    private volatile Snapshot published = Snapshot.EMPTY;

    // Guards in-memory publishes only (never held across metastore I/O), so write-through updates are not
    // blocked while a refresh reads the underlying metastore. While a refresh is in flight, deltasDuringRefresh
    // is non-null and records the write-through deltas applied during the read so the refresh can replay them
    // on top of the freshly listed base instead of clobbering them. Only one refresh runs at a time (the
    // refresh executor is single-threaded and scheduled with a fixed delay), so a single buffer suffices.
    private final ReentrantLock publishLock = new ReentrantLock();
    private List<UnaryOperator<Snapshot>> deltasDuringRefresh;

    private final ScheduledThreadPoolExecutor refreshExecutor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("mv-substitution-index-refresh"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final Duration refreshInterval;
    private final RefreshStats refreshStats = new RefreshStats();

    @Inject
    public MaterializationIndex(VersionAwareMaterializationMetastore materializationMetastore, MaterializedViewSubstitutionConfig config)
    {
        this.materializationMetastore = requireNonNull(materializationMetastore, "materializationMetastore is null");
        this.refreshInterval = config.getMaterializedViewSubstitutionMetastoreRefreshInterval();
    }

    @PostConstruct
    public void start()
    {
        if (!started.getAndSet(true)) {
            refreshExecutor.scheduleWithFixedDelay(() -> {
                try {
                    refresh();
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.warn(e, "Error refreshing materialization index");
                }
            }, 0, refreshInterval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void stop()
    {
        refreshExecutor.shutdownNow();
    }

    public List<MaterializationDefinition> getMaterializations(ComputationHash hash)
    {
        return published.getMaterializations(hash);
    }

    /**
     * Rebuilds the in-memory index from the underlying metastore, picking up materializations created,
     * removed or renamed by other clusters that never flowed through this node's write-through methods.
     * <p>
     * The underlying metastore is read without holding {@link #publishLock} so concurrent write-through
     * updates are not blocked for the duration of the read. Any write-through that lands during the read is
     * recorded in {@link #deltasDuringRefresh} and replayed on top of the freshly listed base before
     * publishing, so it is not lost.
     */
    @VisibleForTesting
    void refresh()
    {
        long startNanos = System.nanoTime();
        try {
            Snapshot previous;
            publishLock.lock();
            try {
                deltasDuringRefresh = new ArrayList<>();
                previous = published;
            }
            finally {
                publishLock.unlock();
            }

            List<MaterializationDefinition> listed = materializationMetastore.listMaterializations();
            Snapshot base = Snapshot.build(listed);

            publishLock.lock();
            try {
                for (UnaryOperator<Snapshot> delta : deltasDuringRefresh) {
                    base = delta.apply(base);
                }
                published = base;
                deltasDuringRefresh = null;
            }
            finally {
                publishLock.unlock();
            }

            RefreshDelta delta = Snapshot.diff(previous, base);
            long elapsedNanos = System.nanoTime() - startNanos;
            refreshStats.recordSuccess(elapsedNanos, listed.size(), delta.total());
            log.debug(
                    "Refreshed materialization index in %s: listed=%s, changed=%s (added=%s, removed=%s, modified=%s)",
                    Duration.succinctNanos(elapsedNanos),
                    listed.size(),
                    delta.total(),
                    delta.added(),
                    delta.removed(),
                    delta.modified());
        }
        catch (Throwable e) {
            refreshStats.recordFailure();
            throw e;
        }
    }

    @Managed
    @Nested
    public RefreshStats getRefreshStats()
    {
        return refreshStats;
    }

    @Override
    public List<MaterializationDefinition> listMaterializations()
    {
        return materializationMetastore.listMaterializations();
    }

    @Override
    public void createOrReplace(MaterializationDefinition materialization)
    {
        materializationMetastore.createOrReplace(materialization);
        publishDelta(snapshot -> snapshot.createOrReplace(materialization));
    }

    @Override
    public void remove(CatalogSchemaTableName materializedViewName)
    {
        materializationMetastore.remove(materializedViewName);
        publishDelta(snapshot -> snapshot.remove(materializedViewName));
    }

    @Override
    public void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
    {
        materializationMetastore.renameIfExists(source, target, targetStorageTableId);
        publishDelta(snapshot -> snapshot.rename(source, target, targetStorageTableId));
    }

    private void publishDelta(UnaryOperator<Snapshot> delta)
    {
        publishLock.lock();
        try {
            Snapshot current = published;
            published = delta.apply(current);
            if (deltasDuringRefresh != null) {
                deltasDuringRefresh.add(delta);
            }
        }
        finally {
            publishLock.unlock();
        }
    }

    static ComputationHash computeHash(Operation root)
    {
        if (root instanceof Output output) {
            root = output.source();
        }
        if (root instanceof TableScan tableScan) {
            return new ComputationHash(tableScan.table().hash());
        }
        throw new UnsupportedOperationException("Unsupported operation: " + root.getClass().getName());
    }

    private static CatalogSchemaTableName materializedViewName(MaterializationDefinition materialization)
    {
        checkArgument(materialization.source() instanceof MaterializedViewSource, "Materialization source is not a materialized view: %s", materialization.source());
        return ((MaterializedViewSource) materialization.source()).materializedViewName();
    }

    private static MaterializationDefinition renameTo(MaterializationDefinition current, CatalogSchemaTableName newName, StorageTableId newStorageTableId)
    {
        return new MaterializationDefinition(
                current.computationPlanRoot(),
                newStorageTableId,
                new MaterializedViewSource(newName),
                current.lastKnownFreshTime(),
                current.gracePeriod());
    }

    public static class RefreshStats
    {
        private final TimeStat time = new TimeStat(MILLISECONDS);
        private final CounterStat listedCount = new CounterStat();
        private final CounterStat refreshedCount = new CounterStat();
        private final CounterStat failureCount = new CounterStat();

        private void recordSuccess(long elapsedNanos, long listedCount, long refreshedCount)
        {
            time.add(elapsedNanos, NANOSECONDS);
            this.listedCount.update(listedCount);
            this.refreshedCount.update(refreshedCount);
        }

        private void recordFailure()
        {
            failureCount.update(1);
        }

        @Managed
        @Nested
        public TimeStat getTime()
        {
            return time;
        }

        @Managed
        @Nested
        public CounterStat getListedCount()
        {
            return listedCount;
        }

        @Managed
        @Nested
        public CounterStat getRefreshedCount()
        {
            return refreshedCount;
        }

        @Managed
        @Nested
        public CounterStat getFailureCount()
        {
            return failureCount;
        }
    }

    private record RefreshDelta(int added, int removed, int modified)
    {
        private int total()
        {
            return added + removed + modified;
        }
    }

    /**
     * Immutable view of the index. {@code byHash} groups materializations by the computation hash queries are
     * matched against; {@code hashByMaterializedViewName} tracks each materialized view's current hash so a definition can be
     * located and moved between buckets when it is replaced, removed or renamed. Mutators return a new
     * {@link Snapshot}, leaving readers of the previous instance unaffected.
     */
    private static class Snapshot
    {
        private static final Snapshot EMPTY = new Snapshot(ImmutableMap.of(), ImmutableMap.of());

        private final Map<ComputationHash, List<MaterializationDefinition>> materializationsByHash;
        private final Map<CatalogSchemaTableName, ComputationHash> hashByMaterializedViewName;

        private Snapshot(Map<ComputationHash, List<MaterializationDefinition>> materializationsByHash, Map<CatalogSchemaTableName, ComputationHash> hashByMaterializedViewName)
        {
            this.materializationsByHash = ImmutableMap.copyOf(materializationsByHash);
            this.hashByMaterializedViewName = ImmutableMap.copyOf(hashByMaterializedViewName);
        }

        private List<MaterializationDefinition> getMaterializations(ComputationHash hash)
        {
            return materializationsByHash.getOrDefault(hash, ImmutableList.of());
        }

        private static RefreshDelta diff(Snapshot previous, Snapshot current)
        {
            Map<CatalogSchemaTableName, MaterializationDefinition> before = previous.byMaterializedViewName();
            Map<CatalogSchemaTableName, MaterializationDefinition> after = current.byMaterializedViewName();
            int added = 0;
            int modified = 0;
            for (Map.Entry<CatalogSchemaTableName, MaterializationDefinition> entry : after.entrySet()) {
                MaterializationDefinition old = before.get(entry.getKey());
                if (old == null) {
                    added++;
                }
                else if (!old.equals(entry.getValue())) {
                    modified++;
                }
            }
            int removed = 0;
            for (CatalogSchemaTableName mvName : before.keySet()) {
                if (!after.containsKey(mvName)) {
                    removed++;
                }
            }
            return new RefreshDelta(added, removed, modified);
        }

        private Map<CatalogSchemaTableName, MaterializationDefinition> byMaterializedViewName()
        {
            ImmutableMap.Builder<CatalogSchemaTableName, MaterializationDefinition> byName = ImmutableMap.builder();
            for (List<MaterializationDefinition> bucket : materializationsByHash.values()) {
                for (MaterializationDefinition materialization : bucket) {
                    byName.put(materializedViewName(materialization), materialization);
                }
            }
            return byName.buildOrThrow();
        }

        private static Snapshot build(List<MaterializationDefinition> materializations)
        {
            Map<ComputationHash, List<MaterializationDefinition>> byHash = new HashMap<>();
            Map<CatalogSchemaTableName, ComputationHash> hashByMaterializedViewName = new HashMap<>();
            for (MaterializationDefinition materialization : materializations) {
                ComputationHash hash = computeHash(materialization.computationPlanRoot());
                byHash.computeIfAbsent(hash, _ -> new ArrayList<>()).add(materialization);
                hashByMaterializedViewName.put(materializedViewName(materialization), hash);
            }
            return new Snapshot(toImmutableBuckets(byHash), ImmutableMap.copyOf(hashByMaterializedViewName));
        }

        private Snapshot createOrReplace(MaterializationDefinition materialization)
        {
            CatalogSchemaTableName mvName = materializedViewName(materialization);
            ComputationHash newHash = computeHash(materialization.computationPlanRoot());
            ComputationHash oldHash = hashByMaterializedViewName.get(mvName);

            Map<ComputationHash, List<MaterializationDefinition>> byHash = new HashMap<>(this.materializationsByHash);
            if (oldHash != null && !oldHash.equals(newHash)) {
                removeFromBucket(byHash, oldHash, mvName);
            }
            List<MaterializationDefinition> bucket = byHash.get(newHash);
            ImmutableList.Builder<MaterializationDefinition> updated = ImmutableList.builder();
            boolean replaced = false;
            if (bucket != null) {
                for (MaterializationDefinition existing : bucket) {
                    if (materializedViewName(existing).equals(mvName)) {
                        updated.add(materialization);
                        replaced = true;
                    }
                    else {
                        updated.add(existing);
                    }
                }
            }
            if (!replaced) {
                updated.add(materialization);
            }
            byHash.put(newHash, updated.build());

            Map<CatalogSchemaTableName, ComputationHash> hashByMaterializedViewName = new HashMap<>(this.hashByMaterializedViewName);
            hashByMaterializedViewName.put(mvName, newHash);
            return new Snapshot(ImmutableMap.copyOf(byHash), ImmutableMap.copyOf(hashByMaterializedViewName));
        }

        private Snapshot remove(CatalogSchemaTableName mvName)
        {
            ComputationHash hash = hashByMaterializedViewName.get(mvName);
            if (hash == null) {
                return this;
            }
            Map<ComputationHash, List<MaterializationDefinition>> byHash = new HashMap<>(this.materializationsByHash);
            removeFromBucket(byHash, hash, mvName);
            Map<CatalogSchemaTableName, ComputationHash> hashByMaterializedViewName = new HashMap<>(this.hashByMaterializedViewName);
            hashByMaterializedViewName.remove(mvName);
            return new Snapshot(ImmutableMap.copyOf(byHash), ImmutableMap.copyOf(hashByMaterializedViewName));
        }

        private Snapshot rename(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
        {
            ComputationHash hash = hashByMaterializedViewName.get(source);
            if (hash == null) {
                return this;
            }
            Map<ComputationHash, List<MaterializationDefinition>> byHash = new HashMap<>(this.materializationsByHash);
            List<MaterializationDefinition> bucket = byHash.get(hash);
            if (bucket != null) {
                ImmutableList.Builder<MaterializationDefinition> updated = ImmutableList.builder();
                for (MaterializationDefinition existing : bucket) {
                    if (materializedViewName(existing).equals(source)) {
                        updated.add(renameTo(existing, target, targetStorageTableId));
                    }
                    else {
                        updated.add(existing);
                    }
                }
                byHash.put(hash, updated.build());
            }
            Map<CatalogSchemaTableName, ComputationHash> hashByMaterializedViewName = new HashMap<>(this.hashByMaterializedViewName);
            hashByMaterializedViewName.remove(source);
            hashByMaterializedViewName.put(target, hash);
            return new Snapshot(ImmutableMap.copyOf(byHash), ImmutableMap.copyOf(hashByMaterializedViewName));
        }

        private static void removeFromBucket(Map<ComputationHash, List<MaterializationDefinition>> byHash, ComputationHash hash, CatalogSchemaTableName mvName)
        {
            List<MaterializationDefinition> bucket = byHash.get(hash);
            if (bucket == null) {
                return;
            }
            List<MaterializationDefinition> filtered = bucket.stream()
                    .filter(materialization -> !materializedViewName(materialization).equals(mvName))
                    .collect(toImmutableList());
            if (filtered.isEmpty()) {
                byHash.remove(hash);
            }
            else {
                byHash.put(hash, filtered);
            }
        }

        private static Map<ComputationHash, List<MaterializationDefinition>> toImmutableBuckets(Map<ComputationHash, List<MaterializationDefinition>> byHash)
        {
            ImmutableMap.Builder<ComputationHash, List<MaterializationDefinition>> immutable = ImmutableMap.builder();
            byHash.forEach((hash, bucket) -> immutable.put(hash, ImmutableList.copyOf(bucket)));
            return immutable.buildOrThrow();
        }
    }
}
