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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryStatusInfo;
import io.trino.client.ResultRows;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;
import org.scharp.sas7bdat.Format;
import org.scharp.sas7bdat.Sas7bdatExporter;
import org.scharp.sas7bdat.Sas7bdatMetadata;
import org.scharp.sas7bdat.Variable;
import org.scharp.sas7bdat.VariableType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class SasTpchLoader
        extends AbstractTestingTrinoClient<Void>
{
    // Days between SAS epoch (1960-01-01) and Java epoch (1970-01-01)
    private static final long SAS_EPOCH_OFFSET = -LocalDate.of(1960, 1, 1).toEpochDay();

    private final Path schemaDir;
    private final String tableName;

    public SasTpchLoader(TestingTrinoServer server, Path schemaDir, String tableName)
    {
        super(server, testSessionBuilder().setCatalog("tpch").setSchema("tiny").build());
        this.schemaDir = requireNonNull(schemaDir, "schemaDir is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @Override
    protected ResultsSession<Void> getResultSession(Session session)
    {
        return new SasLoadingSession(schemaDir, tableName);
    }

    private static final class SasLoadingSession
            implements ResultsSession<Void>
    {
        private final Path schemaDir;
        private final String tableName;
        private final AtomicReference<List<Column>> columns = new AtomicReference<>();
        private final List<List<Object>> rows = new ArrayList<>();

        SasLoadingSession(Path schemaDir, String tableName)
        {
            this.schemaDir = schemaDir;
            this.tableName = tableName;
        }

        @Override
        public void addResults(QueryStatusInfo statusInfo, ResultRows resultRows)
        {
            if (columns.get() == null && statusInfo.getColumns() != null) {
                columns.set(statusInfo.getColumns());
            }
            for (List<Object> row : resultRows) {
                rows.add(convertRow(row, columns.get()));
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            List<Column> cols = requireNonNull(columns.get(), "No columns received from query");
            Sas7bdatMetadata metadata = Sas7bdatMetadata.builder()
                    .datasetName(tableName.toUpperCase(Locale.ROOT))
                    .variables(buildVariables(cols))
                    .build();
            Path outputFile = schemaDir.resolve(tableName + ".sas7bdat");
            try {
                Sas7bdatExporter.exportDataset(outputFile, metadata, rows);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to write SAS7BDAT: " + outputFile, e);
            }
            return null;
        }

        private static List<Variable> buildVariables(List<Column> columns)
        {
            ImmutableList.Builder<Variable> variables = ImmutableList.builder();
            for (Column column : columns) {
                variables.add(toVariable(column));
            }
            return variables.build();
        }

        private static Variable toVariable(Column column)
        {
            String type = column.getType();
            Variable.Builder builder = Variable.builder()
                    .name(column.getName().toUpperCase(Locale.ROOT));

            if (type.equals("date")) {
                // Store as SAS numeric date with MMDDYY format so parso returns java.util.Date
                return builder.type(VariableType.NUMERIC).length(8)
                        .outputFormat(new Format("MMDDYY", 10))
                        .build();
            }
            if (type.startsWith("varchar") || type.startsWith("char")) {
                return builder.type(VariableType.CHARACTER).length(parseVarcharLength(type)).build();
            }
            // bigint, integer, double → all stored as NUMERIC (SAS only has double precision floats)
            return builder.type(VariableType.NUMERIC).length(8).build();
        }

        private static int parseVarcharLength(String type)
        {
            // "varchar(25)" → 25, bare "varchar" → 64
            int start = type.indexOf('(');
            if (start >= 0) {
                int end = type.indexOf(')', start);
                if (end > start) {
                    return Integer.parseInt(type.substring(start + 1, end));
                }
            }
            return 64;
        }

        private static List<Object> convertRow(List<Object> row, List<Column> columns)
        {
            List<Object> converted = new ArrayList<>(row.size());
            for (int i = 0; i < row.size(); i++) {
                converted.add(convertValue(row.get(i), columns.get(i).getType()));
            }
            return converted;
        }

        private static Object convertValue(Object value, String type)
        {
            if (value == null) {
                return null;
            }
            if (type.equals("date")) {
                // HTTP client returns dates as "YYYY-MM-DD" strings; convert to SAS days
                long epochDay = LocalDate.parse(value.toString()).toEpochDay();
                return (double) (epochDay + SAS_EPOCH_OFFSET);
            }
            if (type.startsWith("varchar") || type.startsWith("char")) {
                return value.toString();
            }
            return ((Number) value).doubleValue();
        }
    }
}
