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
package io.trino.plugin.warp.storage.splits;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.node.CoordinatorNodeManager;
import io.trino.spi.Node;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class ConnectorSplitConsistentHashNodeDistributor
        implements ConnectorSplitNodeDistributor
{
    private static final Logger logger = Logger.get(ConnectorSplitConsistentHashNodeDistributor.class);

    private static final long WORKER_NODES_CACHE_TIMEOUT_SECS = 5;

    /**
     * prime number fit for number of nodes 2->128 to test distribution of different values.
     */
    private static final int HASH_PRIME = 173923;

    private final GlobalConfig globalConfig;
    private final CoordinatorNodeManager coordinatorNodeManager;
    private volatile long lastRefreshTime;

    //key = bucket hash, value = node
    private TreeMap<Integer, Node> nodeBucketsTreeMap;
    private int workerNodesHash;

    @Inject
    public ConnectorSplitConsistentHashNodeDistributor(GlobalConfig globalConfig,
            CoordinatorNodeManager coordinatorNodeManager)
    {
        this.globalConfig = requireNonNull(globalConfig);
        this.coordinatorNodeManager = requireNonNull(coordinatorNodeManager);
        this.nodeBucketsTreeMap = new TreeMap<>();
        updateNodeBucketsIfNeeded();
    }

    @SuppressWarnings("unused")
    @Override
    public synchronized void updateNodeBucketsIfNeeded()
    {
        if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
            // Double lock checking pattern to reduce lock contention
            synchronized (this) {
                if (nanosSince(lastRefreshTime).getValue(SECONDS) > WORKER_NODES_CACHE_TIMEOUT_SECS) {
                    updateNodeBuckets();
                }
            }
        }
    }

    @VisibleForTesting
    void updateNodeBuckets()
    {
        lastRefreshTime = System.nanoTime();
        int currentWorkerNodesHash = getWorkerNodesHash();
        if (workerNodesHash == currentWorkerNodesHash) {
            return;
        }
        this.workerNodesHash = currentWorkerNodesHash;
        logger.debug("worker hash changed, reset");
        TreeMap<Integer, Node> nodeBucketsTreeMapTmp = new TreeMap<>();

        List<Node> workers = coordinatorNodeManager.getWorkerNodes();

        IntStream.range(0, globalConfig.getConsistentSplitBucketsPerWorker())
                .boxed()
                .forEach(i -> workers.forEach(node -> {
                    String nodeIdentifier = node.getNodeIdentifier();
                    int bucket = getNodeBucket(nodeIdentifier, i);
                    if (!nodeBucketsTreeMapTmp.containsKey(bucket)) {
                        nodeBucketsTreeMapTmp.put(bucket, node);
                    }
                    else {
                        //ignore conflicting keys
                        logger.debug(format("nodeIdentifier[%s] key %d already exists for nodeIdentifier[%s]",
                                nodeIdentifier,
                                bucket,
                                nodeBucketsTreeMapTmp.get(bucket)));
                    }
                }));
        nodeBucketsTreeMap = nodeBucketsTreeMapTmp;
        logger.debug("consistentHashing is ready for workers [%d]", workers.size());
    }

    int getBucket(String key)
    {
        HashCode hc = Hashing.murmur3_128().hashString(requireNonNull(key), StandardCharsets.UTF_8);
        int hash = hc.hashCode() % HASH_PRIME;
        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }

    public Integer getNodeBucket(String nodeIdentifier, Integer i)
    {
        return getBucket(nodeIdentifier + "###" + i);
    }

    /**
     * gets as input a string key and returns a consistent Node for the key
     */
    @Override
    public Node getNode(String key)
    {
        if (nodeBucketsTreeMap.isEmpty()) {
            throw new IllegalArgumentException("no available nodes");
        }
        Node node;
        int bucket = getBucket(key);

        Map.Entry<Integer, Node> entry;
        /*
         * assume we have in our ring a large bucket.
         * if we always go up, the entire bucket will go to a single node.
         * by doing that we split the large buckets to 2, reducing the effect a bit.
         */
        if (bucket % 2 == 0) {
            entry = nodeBucketsTreeMap.higherEntry(bucket);
        }
        else {
            entry = nodeBucketsTreeMap.floorEntry(bucket);
        }

        if (Objects.isNull(entry)) {
            //If there is no one larger than the hash value of the key,
            // start with the first/last bucket according to the bucket %
            if (bucket % 2 == 0) {
                node = nodeBucketsTreeMap.get(nodeBucketsTreeMap.firstKey());
            }
            else {
                node = nodeBucketsTreeMap.get(nodeBucketsTreeMap.lastKey());
            }
        }
        else {
            //The first Key is the nearest bucket clockwise past the bucket.
            node = entry.getValue();
        }
        return node;
    }

    public int getWorkerNodesHash()
    {
        return coordinatorNodeManager
                .getWorkerNodes()
                .stream()
                .mapToInt(x -> x.getNodeIdentifier().hashCode() ^ x.getHostAndPort().hashCode())
                .sum();
    }
}
