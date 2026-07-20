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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogHandle;
import io.trino.connector.DefaultNodeManager;
import io.trino.execution.scheduler.StableHostAddressProvider;
import io.trino.execution.scheduler.StableHostAddressProviderConfig;
import io.trino.metadata.Split;
import io.trino.node.InternalNode;
import io.trino.node.TestingInternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.cache.CacheSplitId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.cache.SignatureKey;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.metrics.Metrics;
import io.trino.split.SplitSource;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.node.TestingInternalNodeManager.CURRENT_NODE;
import static io.trino.spi.NodeVersion.UNKNOWN;
import static io.trino.spi.cache.PlanSignature.canonicalizePlanSignature;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheSplitSource
{
    private static final PlanSignature SIGNATURE = new PlanSignature(new SignatureKey("signature"), Optional.empty(), ImmutableList.of(), ImmutableList.of());

    @Test
    public void testPlacementFollowsAffinityKeyWhenPresent()
            throws Exception
    {
        StableHostAddressProvider addressProvider = createAddressProvider(8, 2);
        CacheSplitId splitId = new CacheSplitId("split");
        String affinityKey = "s3://bucket/data/file.parquet:0:1024";
        List<HostAddress> affinityHosts = addressProvider.getHosts(affinityKey);
        List<HostAddress> signatureHosts = addressProvider.getHosts(canonicalizePlanSignature(SIGNATURE) + splitId.toString());
        // distinct hosts ensure the assertions below can tell the two placement keys apart
        assertThat(affinityHosts).hasSize(2).isNotEqualTo(signatureHosts);

        Split affinitySplit = new Split(TEST_CATALOG_HANDLE, new AffinityKeySplit(affinityKey));
        Split plainSplit = new Split(TEST_CATALOG_HANDLE, new TestingSplit(true, ImmutableList.of()));
        CacheSplitSource splitSource = new CacheSplitSource(
                SIGNATURE,
                splitManager(splitId),
                new TestingSplitSource(ImmutableList.of(affinitySplit, plainSplit)),
                addressProvider,
                1);

        List<Split> assigned = splitSource.getNextBatch(10).get().getSplits();

        assertThat(assigned).hasSize(2);
        assertThat(assigned.get(0).getCacheSplitId()).contains(splitId);
        // all preferred hosts are attached so scheduling on any of them passes the enforced-address check
        assertThat(assigned.get(0).getAddresses()).isEqualTo(affinityHosts);
        assertThat(assigned.get(1).getAddresses()).isEqualTo(signatureHosts);
    }

    private static ConnectorSplitManager splitManager(CacheSplitId splitId)
    {
        return new ConnectorSplitManager()
        {
            @Override
            public ConnectorSplitSource getSplits(
                    ConnectorTransactionHandle transaction,
                    ConnectorSession session,
                    ConnectorTableHandle table,
                    Set<ColumnHandle> columns,
                    Constraint constraint)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<CacheSplitId> getCacheSplitId(ConnectorSplit split)
            {
                return Optional.of(splitId);
            }
        };
    }

    private static StableHostAddressProvider createAddressProvider(int nodeCount, int preferredHostsCount)
    {
        ImmutableSet.Builder<InternalNode> nodes = ImmutableSet.builder();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(new InternalNode("node" + i, URI.create("http://node" + i + ":8080"), UNKNOWN, false));
        }
        return new StableHostAddressProvider(
                new DefaultNodeManager(CURRENT_NODE, TestingInternalNodeManager.createDefault(nodes.build()), false),
                new StableHostAddressProviderConfig().setPreferredHostsCount(preferredHostsCount));
    }

    private record AffinityKeySplit(String affinityKey)
            implements ConnectorSplit
    {
        @Override
        public Optional<String> getAffinityKey()
        {
            return Optional.of(affinityKey);
        }
    }

    private static class TestingSplitSource
            implements SplitSource
    {
        private final List<Split> splits;
        private boolean finished;

        public TestingSplitSource(List<Split> splits)
        {
            this.splits = ImmutableList.copyOf(splits);
        }

        @Override
        public CatalogHandle getCatalogHandle()
        {
            return TEST_CATALOG_HANDLE;
        }

        @Override
        public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
        {
            finished = true;
            return immediateFuture(new SplitBatch(splits, true));
        }

        @Override
        public void close() {}

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            return Optional.empty();
        }

        @Override
        public Metrics getMetrics()
        {
            return Metrics.EMPTY;
        }
    }
}
