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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StandardJoinFilterFunction
        implements JoinFilterFunction
{
    private static final Page EMPTY_PAGE = new Page(0);

    private final InternalJoinFilterFunction filterFunction;
    private final BlockPositionIndex blockPositionIndex;
    private final List<Page> pages;

    public StandardJoinFilterFunction(InternalJoinFilterFunction filterFunction, BlockPositionIndex blockPositionIndex, List<Page> pages)
    {
        this.filterFunction = requireNonNull(filterFunction, "filterFunction cannot be null");
        this.blockPositionIndex = requireNonNull(blockPositionIndex, "blockPositionIndex is null");
        this.pages = ImmutableList.copyOf(requireNonNull(pages, "pages is null"));
    }

    @Override
    public boolean filter(int leftPosition, int rightPosition, Page rightPage)
    {
        int blockIndex = blockPositionIndex.decodeBlockIndex(leftPosition);
        int blockPosition = blockPositionIndex.decodePosition(leftPosition, blockIndex);

        return filterFunction.filter(blockPosition, pages.isEmpty() ? EMPTY_PAGE : pages.get(blockIndex), rightPosition, rightPage);
    }
}
