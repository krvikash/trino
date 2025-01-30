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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import java.util.Collections;
import java.util.List;

public record TupleRankResult(
        List<TupleRank> tupleRankList,
        List<TupleRank> immediateObjects,
        List<TupleRank> failedObjects)
{
    public TupleRankResult
    {
        Collections.sort(immediateObjects);
        Collections.sort(tupleRankList);
    }

    public double getLowestPriority()
    {
        return tupleRankList().isEmpty() ? Double.MIN_VALUE : tupleRankList().getFirst().warmupProperties().priority();
    }

    public String toShortString()
    {
        return "TupleRankResult{" +
                "tupleRankList.size=" + (tupleRankList != null ? tupleRankList.size() : 0) +
                ", immediateObjects.size=" + (immediateObjects != null ? immediateObjects.size() : 0) +
                ", failedObjects.size=" + (failedObjects != null ? failedObjects.size() : 0) +
                '}';
    }
}
