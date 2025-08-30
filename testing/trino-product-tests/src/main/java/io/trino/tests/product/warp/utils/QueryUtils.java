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
package io.trino.tests.product.warp.utils;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.jdbc.TrinoResultSet;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.SoftAssertions;
import org.intellij.lang.annotations.Language;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.warp.utils.DemoterUtils.objectMapper;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupExportService.EXPORT_ROW_GROUP_FINISHED;
import static io.trino.tests.product.warp.utils.JMXCachingConstants.WarmupExportService.EXPORT_ROW_GROUP_SCHEDULED;
import static io.trino.tests.product.warp.utils.JMXCachingManager.getDiffFromInitial;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class QueryUtils
{
    private static final Logger logger = Logger.get(QueryUtils.class);

    @Inject
    private WarmUtils warmUtils;

    @Inject
    private RuleUtils ruleUtils;

    public QueryUtils()
    {
    }

    public void runQueries(String catalogName, TestFormat test)
    {
        List<TestFormat.QueryData> queriesData = test.queries_data();
        if (queriesData == null) {
            return;
        }
        logger.info("Going to execute %s queries", queriesData.size());
        SoftAssertions softAssertions = new SoftAssertions();
        for (TestFormat.QueryData query : queriesData) {
            if (query.skip()) {
                logger.info("query %s was skipped", query);
            }
            logger.debug("run the query with/without planAlternative");
            onTrino().executeQuery("set session use_sub_plan_alternatives = true");
            queryAndValidate(catalogName, query, true, softAssertions);
            onTrino().executeQuery("set session use_sub_plan_alternatives = false");
            //we assert on correctness without sub_plan_alternatives session, counters according to default config
            queryAndValidate(catalogName, query, false, softAssertions);
        }

        softAssertions.assertAll();
    }

    public int runCacheQueries(TestFormat test, boolean isWarp)
    {
        List<TestFormat.QueryData> queriesData = test.queries_data();
        if (queriesData == null) {
            return 0;
        }
        logger.info("Going to execute %s queries", queriesData.size());
        int ranQueries = 0;
        for (TestFormat.QueryData query : queriesData) {
            if (query.skip() || query.skip_caching()) {
                logger.info("skipping query: %s", query);
                continue;
            }
            warmAndQueryCache(query, test.split_count(), isWarp);
            ranQueries++;
        }
        return ranQueries;
    }

    private void warmAndQueryCache(TestFormat.QueryData queryData, int splitCount, boolean isWarp)
    {
        QueryResult exportRowBefore = null;
        if (isWarp) {
            exportRowBefore = JMXCachingManager.getExportStats();
        }

        AtomicInteger iterationNUmber = new AtomicInteger();
        @Language("SQL") String query = queryData.query();
        assertEventually(
                Duration.valueOf("360s"),
                () -> {
                    logger.info("Running QueryId=%s, split_count=%s, Query=%s", queryData.query_id(), splitCount, query);
                    int iteration = iterationNUmber.getAndIncrement();
                    logger.info("iterationNumber=%s, QueryId=%s", iteration, queryData.query_id());
                    QueryResult queryResult = onTrino().executeQuery(query);
                    List<Object> expectedResult = queryData.expected_result();
                    if (validateQueryResult(expectedResult)) {
                        verifyQueryResult(queryResult, expectedResult, queryData.query_id());
                    }
                    String queryId = ((TrinoResultSet) queryResult.getJdbcResultSet().orElseThrow()).getQueryId();
                    ruleUtils.validateLoadByCacheDataOperator(queryId);
                });

        if (isWarp) {
            logger.info("validate no export occurred");
            QueryResult exportStatsAfter = JMXCachingManager.getExportStats();
            assertThat(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_SCHEDULED))
                    .as("validate no export for cachingManager. queryId=%s", queryData.query_id())
                    .isEqualTo(getDiffFromInitial(exportStatsAfter, exportRowBefore, EXPORT_ROW_GROUP_FINISHED));
        }
    }

    public QueryResult queryAndValidate(@Language("SQL") String query, Map<String, Long> expectedResults, String testName)
    {
        QueryResult queryResult = onTrino().executeQuery(query);
        String queryId = ((TrinoResultSet) queryResult.getJdbcResultSet().orElseThrow()).getQueryId();
        SoftAssertions softAssertions = new SoftAssertions();
        verifyQueryCounters(queryId, expectedResults, CachingType.ACCORDING_TO_COUNTERS, testName, softAssertions);
        softAssertions.assertAll();
        return queryResult;
    }

    private void queryAndValidate(String catalogName, TestFormat.QueryData query, boolean assertOnCounters, SoftAssertions softAssert)
    {
        try {
            @Language("SQL") String queryToExecute = query.query();
            List<Object> expectedResult = query.expected_result();
            Map<String, Long> expectedCounters = query.expected_counters();
            String queryId = query.query_id();
            CachingType cachingType = CachingType.ACCORDING_TO_COUNTERS;

            logger.debug("Going to execute query {%s}, data: {%s}", queryToExecute, query);
            if (query.session_properties() == null || query.session_properties().isEmpty()) {
                logger.info("no seesion properties");
            }
            else {
                Map<String, Object> sessionPropertiesWithCatalog = query.session_properties().entrySet().stream().collect(Collectors.toMap(e -> catalogName + "." + e.getKey(), Map.Entry::getValue));
                warmUtils.setSessions(sessionPropertiesWithCatalog);
            }
            logger.info("Going to execute query: %s", queryToExecute);
            QueryResult queryResult = onTrino().executeQuery(queryToExecute);
            if (queryResult.rows().size() < 10) {
                logger.info("queryId=%s, queryResult=%s", queryId, queryResult.rows());
            }
            if (validateQueryResult(expectedResult)) {
                verifyQueryResult(queryResult, expectedResult, queryId);
                if (assertOnCounters && expectedCounters != null) {
                    verifyQueryCounters(((TrinoResultSet) queryResult.getJdbcResultSet().orElseThrow()).getQueryId(), expectedCounters, cachingType, queryId, softAssert);
                }
            }
        }
        finally {
            if (query.session_properties() == null || query.session_properties().isEmpty()) {
                logger.info("no session properties");
            }
            else {
                Map<String, Object> sessionPropertiesWithCatalog = query.session_properties().entrySet().stream().collect(Collectors.toMap(e -> catalogName + "." + e.getKey(), Map.Entry::getValue));
                warmUtils.resetSessions(sessionPropertiesWithCatalog);
            }
        }
    }

    public static void verifyQueryCountersJson(List<String> countersList,
            Map<String, Long> expectedCounters,
            Map<String, Long> actualValues,
            String testName,
            SoftAssertions softAssertions)
    {
        if (actualValues == null) {
            return;
        }
        // calculate the number of splits
        String counterWithMaxValue = Collections.max(expectedCounters.entrySet(), Map.Entry.comparingByValue()).getKey();
        long maxValue = Collections.max(expectedCounters.values());
        long counterValueInCustomMetrics = 0;
        for (String counter : countersList) {
            if (counter.equals(counterWithMaxValue)) {
                counterValueInCustomMetrics = actualValues.getOrDefault(counter, 0L);
                break;
            }
        }
        double numberOfSplits = (double) counterValueInCustomMetrics / maxValue;
        if (maxValue == 0) {
            numberOfSplits = 0;
        }
        for (String counter : countersList) {
            if (expectedCounters.containsKey(counter)) {
                double counterValue = (double) actualValues.getOrDefault(counter, 0L);
                double expectedValue = expectedCounters.get(counter) * numberOfSplits;
                String info = "testName: " + testName + "; Counter: " + counter + "; Result: " + counterValue + "; Expected: " + expectedValue;
                logger.debug(info);
                softAssertions.assertThat(counterValue)
                        .as("assert failed. actual=%s. expected=%s (%s * %s splits). info=%s, error=%s", counterValue, expectedValue, expectedCounters.get(counter), numberOfSplits, info, JMXCachingManager.getErrorMessage(counter))
                        .isEqualTo(expectedValue);
            }
        }
    }

    public void validateLoadByCacheDataOperator(String queryId)
    {
        ruleUtils.validateLoadByCacheDataOperator(queryId);
    }

    @SuppressWarnings("unchecked")
    private void verifyQueryCounters(
            String queryId,
            Map<String, Long> expectedCounters,
            CachingType cachingType,
            String testName,
            SoftAssertions softAssert)
    {
        if (cachingType == CachingType.ACCORDING_TO_COUNTERS) {
            logger.debug("%s", expectedCounters);
            SetMultimap<String, Object> connectorMetrics = ruleUtils.getCustomStats(queryId, "connectorMetrics");
            Map<String, Long> actualValues = connectorMetrics
                    .entries()
                    .stream()
                    .filter(entry -> expectedCounters.containsKey(entry.getKey()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> {
                                Map<String, Object> map = (Map<String, Object>) entry.getValue();
                                return ((Integer) map.get("total")).longValue();
                            },
                            Long::sum));

            List<String> queryStatsNames = JMXCachingManager.getQueryStatsNames();
            verifyQueryCountersJson(queryStatsNames, expectedCounters, actualValues, testName, softAssert);
        }
    }

    public void verifyQueryResult(QueryResult queryResult, List<Object> expectedResult, String queryId)
    {
        List<JDBCType> columnTypes = queryResult.getColumnTypes();
        List<List<ValueNode>> expectedValuesAsListOfLists = convertExpectedValues(expectedResult);
        int rowsCount = queryResult.getRowsCount();

        assertThat(rowsCount)
                .as("Result contained %d rows while %d rows were expected", queryResult.getRowsCount(), expectedValuesAsListOfLists.size())
                .isEqualTo(expectedValuesAsListOfLists.size());

        for (int rowNumber = 0; rowNumber < rowsCount && !expectedValuesAsListOfLists.isEmpty(); rowNumber++) {
            List<ValueNode> rowResults = expectedValuesAsListOfLists.get(rowNumber);
            for (int columnNumber = 0; columnNumber < columnTypes.size(); columnNumber++) {
                Object actualResultValue = getValue(queryResult, columnNumber, rowNumber);
                Object expectedResultValue = getValue(rowResults.get(columnNumber), columnTypes.get(columnNumber));
                if (actualResultValue instanceof Double actualValue) {
                    Double expectedAsDouble = (Double) expectedResultValue;
                    boolean bothNaN = actualValue.isNaN() && expectedAsDouble.isNaN();
                    double result = bothNaN ? 1 : actualValue > expectedAsDouble ? actualValue / expectedAsDouble : expectedAsDouble / actualValue;
                    assertThat(result > 0.99999999991 && result <= 1)
                            .as("result=%s is not equal to expectedAsDouble=%s. queryId=%s", result, expectedAsDouble, queryId)
                            .isTrue();
                }
                else if (actualResultValue instanceof Float actualValue) {
                    float expectedAsFloat = ((Double) expectedResultValue).floatValue();
                    assertThat(actualValue)
                            .as("actualResultValue=%s is not equal to expectedResultValue=%s. queryId=%s", actualResultValue, expectedResultValue, queryId)
                            .isEqualTo(expectedAsFloat);
                }
                else {
                    assertThat(actualResultValue)
                            .as("actualResultValue=%s is not equal to expectedResultValue=%s. queryId=%s", actualResultValue, expectedResultValue, queryId)
                            .isEqualTo(expectedResultValue);
                }
            }
        }
    }

    public Double getTableRowCount(String catalogName, String schemaName, String tableName)
    {
        QueryResult queryResult = onTrino().executeQuery(format("SELECT COUNT(*) FROM %s.%s.%s", catalogName, schemaName, tableName));
        return Double.parseDouble(queryResult.getOnlyValue().toString());
    }

    private Object getValue(QueryResult queryResult, int columnNumber, int rowNumber)
    {
        Object res = queryResult.column(columnNumber + 1).get(rowNumber);
        if (res instanceof BigDecimal bigDecimal) {
            res = bigDecimal.doubleValue();
        }
        else if (res == null) {
            res = NullNode.getInstance();
        }
        return res;
    }

    private boolean validateQueryResult(List<Object> expectedResult)
    {
        return expectedResult != null && !expectedResult.isEmpty() && !String.valueOf(expectedResult.getFirst()).toUpperCase(Locale.ROOT).equals("IGNORE");
    }

    private Object getValue(ValueNode expectedResult, JDBCType columnType)
    {
        if (expectedResult == null) {
            throw new UnsupportedOperationException();
        }
        if (expectedResult instanceof NullNode) {
            return expectedResult;
        }

        return switch (columnType) {
            case VARCHAR, CHAR, LONGNVARCHAR -> expectedResult.asText();
            case DECIMAL, FLOAT, DOUBLE, REAL -> expectedResult.asDouble();
            case BIGINT -> expectedResult.asLong();
            case INTEGER -> expectedResult.asInt();
            case BOOLEAN -> expectedResult.asBoolean();
            case SMALLINT -> (short) expectedResult.asInt();
            case TINYINT -> (byte) expectedResult.asInt();
            case TIMESTAMP -> Timestamp.valueOf(expectedResult.asText());
            case TIMESTAMP_WITH_TIMEZONE -> {
                // Parse the UTC timestamp
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS 'UTC'").withZone(ZoneOffset.UTC);
                LocalDateTime utcDateTime = LocalDateTime.parse(expectedResult.asText(), formatter);
                // Calculate and apply timezone offset, this is the docker time offset
                ZoneOffset dockerOffset = ZoneOffset.ofHoursMinutes(5, 45);
                ZonedDateTime resultDateTime = utcDateTime.atZone(ZoneOffset.UTC).withZoneSameInstant(dockerOffset);
                // Format the timestamp in desired format
                yield Timestamp.valueOf(resultDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")));
            }
            case DATE -> Date.valueOf(expectedResult.asText());
/*            case ARRAY -> {
                String value = expectedResult.asText();
                yield new ArrayType(RowType.from(ImmutableList.of(
                        RowType.field(value, VarcharType.VARCHAR))));
            }*/
            default -> throw new UnsupportedOperationException();
        };
    }

    @SuppressWarnings("unchecked")
    private List<List<ValueNode>> convertExpectedValues(List<Object> expectedValues)
    {
        //ArrayNode arrayNode = (ArrayNode) expectedValues; // Assuming the JSON object is an array of objects
        List<List<ValueNode>> arrayOfArrays = new ArrayList<>(); // The resulting array of arrays
        if (expectedValues.getFirst() instanceof List) {
            //array of arrays
            for (Object rowValue : expectedValues) {
                List<ValueNode> values = convertRowToValues((List<Object>) rowValue);
                arrayOfArrays.add(values); // Add the array to the list of arrays
            }
        }
        else {
            //single line
            List<ValueNode> values = convertRowToValues(expectedValues);
            arrayOfArrays.add(values);
        }
        return arrayOfArrays;
    }

    private List<ValueNode> convertRowToValues(List<Object> expectedValues)
    {
        List<ValueNode> values = new ArrayList<>();
        for (Object expectedValue : expectedValues) {
            ValueNode value = objectMapper.convertValue(expectedValue, ValueNode.class);
            values.add(value);
        }
        return values;
    }

    public boolean isTableExists(String schema, String table)
    {
        QueryResult queryResult = onTrino().executeQuery("show tables from " + schema);
        if (queryResult.getRowsCount() == 0) {
            return false;
        }

        for (int rowNumber = 0; rowNumber < queryResult.getRowsCount(); rowNumber++) {
            String name = (String) queryResult.column(1).get(rowNumber);
            if (name.equals(table)) {
                return true;
            }
        }
        return false;
    }

    private enum CachingType
    {
        WARP_ONLY,
        EXTERNAL_ONLY,
        ACCORDING_TO_COUNTERS,
        MIXED_WARP_AND_EXTERNAL_NO_COUNTERS,
        CUSTOM_METRICS_COUNTERS
    }
}
