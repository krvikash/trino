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
package io.trino.spi.block;

import static java.util.Objects.requireNonNull;

/**
 * Wrapper around BlockBuilder which implements FixedSizeBlockBuilder.
 * This is useful for Types where a FixedSizeBlockBuilder is not explicitly implemented.
 */
public class DefaultPreSizedBlockBuilder
        implements PreSizedBlockBuilder
{
    private final BlockBuilder blockBuilder;

    public DefaultPreSizedBlockBuilder(BlockBuilder blockBuilder)
    {
        requireNonNull(blockBuilder, "blockBuilder is null");
        this.blockBuilder = blockBuilder;
    }

    @Override
    public void appendNull()
    {
        blockBuilder.appendNull();
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        blockBuilder.append(block, position);
    }

    @Override
    public Block build()
    {
        return blockBuilder.build();
    }

    public BlockBuilder getBlockBuilder()
    {
        return blockBuilder;
    }
}
