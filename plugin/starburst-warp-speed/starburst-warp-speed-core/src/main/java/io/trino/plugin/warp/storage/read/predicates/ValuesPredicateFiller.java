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
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;

public class ValuesPredicateFiller
        extends PredicateFiller
{
    private static final Logger logger = Logger.get(ValuesPredicateFiller.class);

    public ValuesPredicateFiller(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator);
    }

    @Override
    public void fillPredicate(Domain domain, ByteBuffer predicateBuffer, PredicateData predicateData)
    {
        predicateBuffer = writePredicateInfoToBuffer(predicateBuffer, predicateData);
        convertValues(predicateBuffer, domain);
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_VALUES;
    }

    @Override
    public void convertValues(Domain domain, ByteBuffer predicateBuffer)
    {
        ByteBuffer predicateBufferVals = bufferAllocator.createBuffView(predicateBuffer);
        convertValues(predicateBufferVals, domain);
    }

    // lower value and high value are equal in valuesPredicate (singleValue), so it enough to take only one value
    protected void convertValues(ByteBuffer buf, Domain domain)
    {
        Type type = domain.getType();
        try {
            Block sortedRangesBlock = ((SortedRangeSet) domain.getValues()).getSortedRanges();
            writeValues(sortedRangesBlock, type, buf, 0, sortedRangesBlock.getPositionCount());
        }
        catch (Exception e) {
            logger.error(e, "convertValues failed type=%s", type);
            throw e;
        }
    }
}
