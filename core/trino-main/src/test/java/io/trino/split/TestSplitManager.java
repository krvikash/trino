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
package io.trino.split;

import com.google.common.collect.ImmutableList;
import io.trino.node.InternalNode;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeVersion;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static io.trino.split.SplitManager.resolveWorkers;
import static org.assertj.core.api.Assertions.assertThat;

final class TestSplitManager
{
    private static final List<InternalNode> WORKERS = ImmutableList.of(
            worker(0),
            worker(1),
            worker(2),
            worker(3),
            worker(4));

    @Test
    void testRankedWorkersLeadInRingOrder()
    {
        List<HostAddress> rankedWorkers = ImmutableList.of(hostOf(2), hostOf(0));

        // ranked workers lead in ring order, the rest follow in stable order
        assertThat(resolveWorkers(WORKERS, rankedWorkers))
                .containsExactly(uriOf(2), uriOf(0), uriOf(1), uriOf(3), uriOf(4));

        // deterministic regardless of how the node manager ordered the worker list
        assertThat(resolveWorkers(WORKERS.reversed(), rankedWorkers))
                .isEqualTo(resolveWorkers(WORKERS, rankedWorkers));
    }

    @Test
    void testStaleRankedHostIsSkipped()
    {
        // the ring refreshes lazily, so it may rank a host that is no longer an active worker
        assertThat(resolveWorkers(WORKERS, ImmutableList.of(HostAddress.fromParts("gone", 8080), hostOf(3))))
                .containsExactly(uriOf(3), uriOf(0), uriOf(1), uriOf(2), uriOf(4));
    }

    @Test
    void testNoAffinityKeepsAllWorkers()
    {
        assertThat(resolveWorkers(WORKERS, ImmutableList.of()))
                .containsExactlyInAnyOrderElementsOf(WORKERS.stream().map(InternalNode::getInternalUri).toList());
    }

    private static InternalNode worker(int index)
    {
        return new InternalNode("node-" + index, URI.create("http://worker-" + index + ":8080"), new NodeVersion("test"), false);
    }

    private static HostAddress hostOf(int index)
    {
        return worker(index).getHostAndPort();
    }

    private static URI uriOf(int index)
    {
        return worker(index).getInternalUri();
    }
}
