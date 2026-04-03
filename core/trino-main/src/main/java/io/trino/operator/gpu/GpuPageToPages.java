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
package io.trino.operator.gpu;

import io.trino.operator.gpu.borrow.Borrow;
import io.trino.spi.Page;
import io.trino.spi.block.Block;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;

public class GpuPageToPages
{
    private final Deque<Page> pendingOutputPages = new ArrayDeque<>();

    public void add(@Borrow GpuPage gpuPage)
    {
        if (gpuPage.columnCount() == 0) {
            pendingOutputPages.addLast(new Page(gpuPage.positionCount()));
            return;
        }

        for (@Borrow Column column : gpuPage.columns()) {
            checkState(column instanceof Column.Blocks, "Unexpected column: %s", column);
            checkState(
                    ((Column.Blocks) gpuPage.column(0)).blocks().size() == ((Column.Blocks) column).blocks().size(),
                    "Block count not aligned between columns");
        }

        for (int pageNumber = 0; pageNumber < ((Column.Blocks) gpuPage.column(0)).blocks().size(); pageNumber++) {
            Block[] pageBlocks = new Block[gpuPage.columnCount()];
            for (int column = 0; column < gpuPage.columnCount(); column++) {
                pageBlocks[column] = ((Column.Blocks) gpuPage.column(column)).blocks().get(pageNumber);
            }
            for (Block block : pageBlocks) {
                checkState(
                        pageBlocks[0].getPositionCount() == block.getPositionCount(),
                        "Block size not aligned between columns");
            }
            pendingOutputPages.addLast(new Page(pageBlocks));
        }
    }

    public Optional<Page> poll()
    {
        return Optional.ofNullable(pendingOutputPages.pollFirst());
    }

    public Stream<Page> drain()
    {
        return Stream.generate(this::poll)
                .takeWhile(Optional::isPresent)
                .flatMap(Optional::stream);
    }
}
