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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogHandle;
import io.trino.execution.scheduler.StableHostAddressProvider;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.subquery.cache.CacheSplitId;
import io.trino.spi.subquery.cache.PlanSignature;
import io.trino.spi.subquery.cache.SubqueryCacheManager;
import io.trino.split.SplitSource;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.spi.subquery.cache.PlanSignature.canonicalizePlanSignature;
import static java.util.Objects.requireNonNull;

/**
 * Assigns addresses provided by {@link SubqueryCacheManager} to splits that
 * are to be cached.
 */
public class CacheSplitSource
        implements SplitSource
{
    private final ConnectorSplitManager splitManager;
    private final SplitSource delegate;
    private final StableHostAddressProvider addressProvider;
    private final String canonicalSignature;

    public CacheSplitSource(
            PlanSignature signature,
            ConnectorSplitManager splitManager,
            SplitSource delegate,
            StableHostAddressProvider addressProvider)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.addressProvider = requireNonNull(addressProvider, "addressProvider is null");
        this.canonicalSignature = canonicalizePlanSignature(signature).toString();
    }

    @Override
    public CatalogHandle getCatalogHandle()
    {
        return delegate.getCatalogHandle();
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
    {
        return transform(delegate.getNextBatch(maxSize), this::assignAddresses, directExecutor());
    }

    private SplitBatch assignAddresses(SplitBatch batch)
    {
        List<Split> splits = batch.getSplits().stream()
                .map(this::assignAddress)
                .collect(toImmutableList());
        return new SplitBatch(splits, batch.isLastBatch());
    }

    private Split assignAddress(Split split)
    {
        Optional<CacheSplitId> splitId = splitManager.getCacheSplitId(split.getConnectorSplit());
        if (splitId.isEmpty()) {
            return split;
        }

        List<HostAddress> preferredAddresses;
        if (!split.isRemotelyAccessible()) {
            checkArgument(!split.getAddresses().isEmpty(), "Split is not remotely accessible but has no addresses: %s", split);
            // Choose first address from connector provided worker addresses, so that split is
            // scheduled deterministically on the worker node. This is such that we reuse the cached splits
            // on the worker nodes.
            preferredAddresses = ImmutableList.of(split.getAddresses().getFirst());
        }
        else {
            // The scheduler routes splits with an affinity key by that key, so the enforced-address
            // check in HttpRemoteTask must accept every host the scheduler may pick;
            // otherwise place by signature and split id.
            String placementKey = split.getConnectorSplit().getAffinityKey()
                    .orElseGet(() -> canonicalSignature + splitId.orElseThrow());
            preferredAddresses = addressProvider.getHosts(placementKey);
        }

        if (preferredAddresses.isEmpty()) {
            // Skip caching if no preferred address could be located which could be due to no available nodes
            return split;
        }
        return new Split(
                split.getCatalogHandle(),
                split.getConnectorSplit(),
                splitId,
                Optional.of(preferredAddresses),
                split.isSplitAddressEnforced());
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        return delegate.getTableExecuteSplitsInfo();
    }

    @Override
    public Metrics getMetrics()
    {
        // todo add some cache specific metrics
        return delegate.getMetrics();
    }
}
