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
package io.trino.plugin.warp.storage.read;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.storage.memory.GcArena;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryParams
{
    private final Optional<MatchNode> rootMatchNode;
    private final Optional<MemorySegment> warmUpElementMatchParams;
    private final MemorySegment matchNodeAtts;
    private final Optional<MemorySegment> warmUpElementCollectParams;
    private final List<WarmupElementMatchParams> leaves;
    private final int numLucene;
    private final int matchCollectId;
    private final List<WarmupElementCollectParams> collectParams;
    private final int numLoadDataValues;
    private final int numMatchCollect;
    private final int totalNumRecords;
    private final int minMatchOffset;
    private final int minCollectOffset;
    private final ImmutableList<PredicateCacheData> predicateCacheData;
    private final boolean rangesRequired;
    private final String queryId;
    private final GcArena arena;

    private final String filePath;
    private final long fileModTime;
    private final long rowGroupUniqueId; // TBD - will be used for logs, currently zero

    public QueryParams(Optional<MatchNode> rootMatchNode,
            Optional<MemorySegment> warmUpElementMatchParams,
            MemorySegment matchNodeAtts,
            Optional<MemorySegment> warmUpElementCollectParams,
            int numLucene,
            int matchCollectId,
            List<WarmupElementCollectParams> collectParams,
            int totalNumRecords,
            int minMatchOffset,
            int minCollectOffset,
            String filePath,
            long fileModTime,
            ImmutableList<PredicateCacheData> predicateCacheData,
            boolean rangesRequired,
            GcArena arena,
            String queryId)
    {
        this.rootMatchNode = requireNonNull(rootMatchNode, "rootMatchNode is null");
        this.warmUpElementMatchParams = requireNonNull(warmUpElementMatchParams);
        this.matchNodeAtts = requireNonNull(matchNodeAtts);
        this.warmUpElementCollectParams = requireNonNull(warmUpElementCollectParams);
        this.leaves = rootMatchNode.map(this::getLeaves).orElse(Collections.emptyList());
        this.numLucene = numLucene;
        this.matchCollectId = matchCollectId;
        this.collectParams = requireNonNull(collectParams, "collectParams is null");
        this.numLoadDataValues = (int) collectParams.stream()
                .filter(WarmupElementCollectParams::hasDictionaryParams)
                .count();
        this.numMatchCollect = (int) collectParams.stream()
                .filter(WarmupElementCollectParams::hasMatchCollect)
                .count();
        this.totalNumRecords = totalNumRecords;
        this.minMatchOffset = minMatchOffset;
        this.minCollectOffset = minCollectOffset;
        this.predicateCacheData = predicateCacheData;
        this.rangesRequired = rangesRequired;
        this.queryId = queryId;
        //TODO hash of file path
        this.rowGroupUniqueId = Calendar.getInstance().getTimeInMillis();
        this.filePath = filePath;
        this.fileModTime = fileModTime;
        this.arena = arena;
    }

    private List<WarmupElementMatchParams> getLeaves(MatchNode node)
    {
        if (node instanceof WarmupElementMatchParams warmupElementMatchParams) {
            return List.of(warmupElementMatchParams);
        }
        List<WarmupElementMatchParams> res = new ArrayList<>();
        for (MatchNode child : node.getChildren()) {
            if (child instanceof WarmupElementMatchParams warmupElementMatchParams) {
                res.add(warmupElementMatchParams);
            }
            else {
                res.addAll(getLeaves(child));
            }
        }
        return res;
    }

    public List<WarmupElementMatchParams> getMatchElementsParamsList()
    {
        return leaves;
    }

    public List<WarmupElementCollectParams> getCollectElementsParamsList()
    {
        return collectParams;
    }

    public int getNumMatchElements()
    {
        return leaves.size();
    }

    public int getNumCollectElements()
    {
        return collectParams.size();
    }

    public int getNumLucene()
    {
        return numLucene;
    }

    public int getMatchCollectId()
    {
        return matchCollectId;
    }

    public int getNumLoadDataValues()
    {
        return numLoadDataValues;
    }

    public int getNumMatchCollect()
    {
        return numMatchCollect;
    }

    public int getTotalNumRecords()
    {
        return totalNumRecords;
    }

    public String getFilePath()
    {
        return filePath;
    }

    public long getFileModTime()
    {
        return fileModTime;
    }

    public int getMinMatchOffset()
    {
        return minMatchOffset;
    }

    public int getMinCollectOffset()
    {
        return minCollectOffset;
    }

    public GcArena getArena()
    {
        return arena;
    }

    public Optional<MemorySegment> getWarmUpElementMatchParams()
    {
        return warmUpElementMatchParams;
    }

    public Optional<MemorySegment> getWarmUpElementCollectParams()
    {
        return warmUpElementCollectParams;
    }

    public MemorySegment getMatchNodeAtts()
    {
        return matchNodeAtts;
    }

    public int getMatchTreeHeight()
    {
        return rootMatchNode.map(r -> r.getHeight()).orElse(0);
    }

    public ImmutableList<PredicateCacheData> getPredicateCacheData()
    {
        return predicateCacheData;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rootMatchNode, collectParams, rowGroupUniqueId);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof QueryParams o)) {
            return false;
        }

        return rootMatchNode.equals(o.rootMatchNode) &&
                collectParams.equals(o.collectParams) &&
                rowGroupUniqueId == o.rowGroupUniqueId;
    }

    @Override
    public String toString()
    {
        return String.format(Locale.US, "rootMatchNode %s collectParamsList %s numLoadDataValues %d numLucene %d",
                rootMatchNode, collectParams, numLoadDataValues, numLucene);
    }

    public Optional<MatchNode> getRootMatchNode()
    {
        return rootMatchNode;
    }

    public boolean isRangesRequired()
    {
        return rangesRequired;
    }

    public String getQueryId()
    {
        return queryId;
    }
}
