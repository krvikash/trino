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
package io.trino.plugin.warp.storage.read.predicates;

import io.airlift.log.Logger;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.plugin.warp.util.StringPredicateData;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;

import java.nio.ByteBuffer;
import java.util.List;

public class InverseStringValuesPredicateFiller
        extends StringValuesPredicateFiller
{
    private static final Logger logger = Logger.get(StringValuesPredicateFiller.class);

    public InverseStringValuesPredicateFiller(StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator)
    {
        super(storageEngineConstants, bufferAllocator);
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_INVERSE_STRING;
    }

    @Override
    void convertString(Domain domain, ByteBuffer lowBuf, ByteBuffer highBuf)
    {
        try {
            int numValues = ((SortedRangeSet) domain.getValues()).getRangeCount() - 1;
            lowBuf.position(0);
            highBuf.position(Long.BYTES * numValues);

            SortedRangeSet sortedRangeSet = (SortedRangeSet) domain.getValues();
            // each value has low, high. upper bound of one range equals the lower bound of the next range. Starts with MIN and ends with MAX
            List<StringPredicateData> strList = SliceUtils.getOrderedStringData(sortedRangeSet, numValues, storageEngineConstants);

            for (StringPredicateData str : strList) {
                lowBuf.putLong(str.comperationValue());
                highBuf.putLong(SliceUtils.str2int(str.value(), str.length(), true));
            }
        }
        catch (Exception e) {
            logger.error(e, "convertString failed");
            throw e;
        }
    }
}
