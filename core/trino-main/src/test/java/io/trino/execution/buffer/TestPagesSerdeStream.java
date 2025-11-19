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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import io.trino.FeaturesConfig;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.InternalBlockEncodingSerde;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.PageStreamFactory;
import io.trino.spi.PageStreamReader;
import io.trino.spi.PageStreamWriter;
import io.trino.spi.type.Type;
import io.trino.tpch.LineItem;
import io.trino.tpch.LineItemGenerator;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.newInputStream;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPagesSerdeStream
{
    @Test
    void testRoundTrip()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        List<Page> inputPages = generatePages(types);
        Path spillFile = Files.createTempFile(TestPagesSerdeStream.class.getSimpleName(), null);
        try {
            InternalBlockEncodingSerde blockEncodingSerde = new InternalBlockEncodingSerde(new BlockEncodingManager(new FeaturesConfig(), new BlockEncodingSimdSupport(true)), TESTING_TYPE_MANAGER);
            PageStreamFactory factory = new PagesSerdeStreamFactory(blockEncodingSerde);

            PageStreamWriter writer = factory.createWriter(Files.newOutputStream(spillFile));
            for (Page page : inputPages) {
                writer.writePage(page);
            }
            writer.close();

            PageStreamReader reader = factory.createReader(newInputStream(spillFile));
            List<Page> readPages = ImmutableList.copyOf(reader);
            assertThat(readPages.size()).isEqualTo(inputPages.size());
            for (int i = 0; i < inputPages.size(); ++i) {
                Page page = inputPages.get(i);
                assertPageEquals(types, page, readPages.get(i));
            }
            reader.close();
        }
        finally {
            Files.delete(spillFile);
        }
    }

    private static List<Page> generatePages(List<Type> types)
    {
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();

        int pageCount = 100;
        int rowCount = 1000;

        return IntStream.range(0, pageCount)
                .mapToObj(_ -> generatePage(types, rowCount, iterator))
                .collect(toImmutableList());
    }

    private static Page generatePage(List<Type> types, int rowCount, Iterator<LineItem> iterator)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        for (int row = 0; row < rowCount; row++) {
            pageBuilder.declarePosition();
            LineItem lineItem = iterator.next();
            for (int column = 0; column < types.size(); column++) {
                Type type = types.get(column);
                if (BIGINT.equals(type)) {
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(column), lineItem.orderKey());
                }
                else if (VARCHAR.equals(type)) {
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(column), lineItem.comment());
                }
                else if (DOUBLE.equals(type)) {
                    DOUBLE.writeDouble(pageBuilder.getBlockBuilder(column), lineItem.extendedPrice());
                }
                else {
                    throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }
        }
        return pageBuilder.build();
    }
}
