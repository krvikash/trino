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
package io.trino.plugin.iceberg;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergFileFormat.fromIceberg;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.apache.iceberg.PartitionStatisticsWriter.convertTrinoValueToIceberg;
import static org.apache.iceberg.TableUtil.formatVersion;

/**
 * Reader for <a href="https://iceberg.apache.org/spec/#partition-statistics">partition statistics</a>
 */
public final class PartitionStatisticsReader
{
    private final TypeManager typeManager;
    private final IcebergPageSourceProviderFactory pageSourceProviderFactory;

    @Inject
    public PartitionStatisticsReader(
            TypeManager typeManager,
            IcebergPageSourceProviderFactory pageSourceProviderFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
    }

    @MustBeClosed
    public PartitionStatsIterator readPartitionStats(ConnectorSession session, Table table, Schema schema, InputFile inputFile)
    {
        FileFormat fileFormat = FileFormat.fromFileName(inputFile.location());
        IcebergPageSourceProvider pageSourceProvider = pageSourceProviderFactory.createPageSourceProvider();
        // IcebergUtil.getProjectedColumns method adds partition columns. It leads to failure when reading the page
        List<IcebergColumnHandle> projectedColumns = schema.columns().stream()
                .map(column -> getColumnHandle(column, typeManager))
                .collect(toImmutableList());

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                session,
                projectedColumns,
                schema,
                unpartitioned(),
                new PartitionData(new Object[] {}),
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                // TODO Rewrite the actual predicate to match "partition" struct in partition stats files
                TupleDomain.all(),
                TupleDomain.all(),
                inputFile.location(),
                0,
                inputFile.getLength(),
                inputFile.getLength(),
                0,
                null,
                fromIceberg(fileFormat),
                ImmutableMap.of(),
                null,
                null,
                Optional.empty(),
                formatVersion(table),
                false);

        return new PartitionStatsIterator(pageSource, schema);
    }

    public final class PartitionStatsIterator
            extends AbstractIterator<PartitionStats>
            implements AutoCloseable
    {
        private final ConnectorPageSource pageSource;
        private final Schema schema;
        private SourcePage currentPage;
        private int currentPosition;

        private PartitionStatsIterator(ConnectorPageSource pageSource, Schema schema)
        {
            this.pageSource = requireNonNull(pageSource, "pageSource is null");
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        protected PartitionStats computeNext()
        {
            while (currentPage == null || currentPosition >= currentPage.getPositionCount()) {
                if (pageSource.isFinished()) {
                    return endOfData();
                }
                currentPage = pageSource.getNextSourcePage();
                currentPosition = 0;
                if (currentPage == null) {
                    continue;
                }
                if (currentPage.getPositionCount() == 0) {
                    currentPage = null;
                }
            }

            GenericRecord record = GenericRecord.create(schema);
            for (int column = 0; column < schema.columns().size(); column++) {
                Types.NestedField field = schema.columns().get(column);
                org.apache.iceberg.types.Type icebergType = field.type();
                Type trinoType = toTrinoType(icebergType, typeManager);
                Object trinoValue = readNativeValue(trinoType, currentPage.getBlock(column), currentPosition);
                record.set(column, convertTrinoValueToIceberg(icebergType, trinoType, trinoValue, currentPosition));
            }
            currentPosition++;
            return toPartitionStats(record);
        }

        @Override
        public void close()
        {
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static PartitionStats toPartitionStats(StructLike record)
    {
        StructLike partition = record.get(0, StructLike.class);
        Integer specId = record.get(1, Integer.class);
        PartitionStats stats = new PartitionStats(partition, specId);
        for (int i = 2; i < record.size(); i++) {
            stats.set(i, record.get(i, Object.class));
        }
        return stats;
    }
}
