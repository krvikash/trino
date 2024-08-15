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

import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.spi.predicate.Domain;

import java.nio.ByteBuffer;

public class LucenePredicateFiller
        extends PredicateFiller
{
    public LucenePredicateFiller(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator);
    }

    @Override
    public void fillPredicate(Domain domain, ByteBuffer predicateBuffer, PredicateData predicateData)
    {
        writePredicateInfoToBuffer(predicateBuffer, predicateData);
    }

    @Override
    public void convertValues(Domain domain, ByteBuffer predicateBuffer)
    {
    }

    @Override
    public PredicateType getPredicateType()
    {
        return PredicateType.PREDICATE_TYPE_LUCENE;
    }
}
