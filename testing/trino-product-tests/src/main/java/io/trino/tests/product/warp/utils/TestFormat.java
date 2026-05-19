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

import io.trino.plugin.warp.api.warmup.WarmUpType;
import io.trino.plugin.warp.api.warmup.WarmupPredicateRule;
import io.trino.tests.product.warp.utils.syntheticconfig.TableType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public record TestFormat(
        String name,
        int lines,
        String table_name,
        List<Column> structure,
        String data_format,
        List<WarmupRule> warmup_rules,
        Map<String, Object> session_properties,
        String warm_query,
        boolean skip,
        boolean pt_enable,
        boolean skip_caching,
        List<QueryData> queries_data,
        WarmTypeForStrings warm_type_for_strings,
        String description,
        int expected_warm_failures,
        List<String> failed_warmup_elements,
        Map<String, Long> expected_dictionary_counters,
        Set<TableType> skip_type,
        Map<String, Long> iceberg_expected_dictionary_counters,
        Map<String, Long> dl_expected_dictionary_counters,
        int split_count,
        List<String> partition_by,
        List<Object> bucketed_by,
        int bucket_count,
        Optional<String> orig_table_name,
        Map<String, TestFormat> overriding)
{
    public record Column(String name, String type, List<Object> args) {}

    public record WarmupRule(
            String colNameId,
            List<WarmupPredicateRule> predicates,
            List<WarmUpType> warmUpTypes,
            int priority,
            Duration ttl) {}

    public record QueryData(
            String query,
            List<Object> expected_result,
            String query_id,
            Map<String, Long> expected_counters,
            Map<String, Object> session_properties,
            boolean skip,
            List<Object> expected_iceberg_result,
            List<Object> expected_dl_result,
            Map<String, Long> iceberg_expected_counters,
            Map<String, Long> dl_expected_counters,
            boolean skip_caching) {}

    public String getTableName()
    {
        return table_name() != null ? table_name() : name();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(TestFormat testFormat)
    {
        return new Builder()
                .name(testFormat.name())
                .lines(testFormat.lines())
                .tableName(testFormat.table_name())
                .structure(testFormat.structure())
                .dataFormat(testFormat.data_format())
                .warmupRules(testFormat.warmup_rules())
                .sessionProperties(testFormat.session_properties())
                .warmQuery(testFormat.warm_query())
                .skip(testFormat.skip())
                .ptEnable(testFormat.pt_enable())
                .skipCaching(testFormat.skip_caching())
                .queriesData(testFormat.queries_data())
                .warmTypeForStrings(testFormat.warm_type_for_strings())
                .description(testFormat.description())
                .splitCount(testFormat.split_count())
                .partitionBy(testFormat.partition_by())
                .bucketCount(testFormat.bucket_count())
                .bucketedBy(testFormat.bucketed_by())
                .expectedWarmFailures(testFormat.expected_warm_failures())
                .expectedDictionaryCounters(testFormat.expected_dictionary_counters())
                .skipType(testFormat.skip_type())
                .expectedIcebergDictionaryCounters(testFormat.iceberg_expected_dictionary_counters())
                .expectedDLDictionaryCounters(testFormat.dl_expected_dictionary_counters())
                .overriding(testFormat.overriding())
                .origTableName(testFormat.orig_table_name());
    }

    // The test format that we got from the json, should be changed in case table type other than warp (DL or Iceberg)
    // was provided in the test command line
    // The table name, expected results and expected counters sometimes are affect by table type
    public TestFormat withTableType(String newTableName, TableType tableType)
    {
        List<QueryData> updatedQueriesData = new ArrayList<>();
        Map<String, Long> updatedDictionaryCounters = expected_dictionary_counters;

        for (QueryData queryData : queries_data()) {
            updatedQueriesData.add(getUpdatedQueryData(tableType, queryData, newTableName));
        }

        String updatedWarmQuery = null;
        if (warm_query != null) {
            updatedWarmQuery = warm_query.replace(getTableName(), newTableName);
        }

        if (tableType == TableType.warp_delta_lake && dl_expected_dictionary_counters != null) {
            updatedDictionaryCounters = dl_expected_dictionary_counters;
        }
        else if (tableType == TableType.warp_iceberg && iceberg_expected_dictionary_counters != null) {
            updatedDictionaryCounters = iceberg_expected_dictionary_counters;
        }

        return builder(this)
                .name(newTableName)
                .tableName(newTableName)
                .origTableName(Optional.ofNullable(getTableName()))
                .warmQuery(updatedWarmQuery)
                .queriesData(updatedQueriesData)
                .expectedDictionaryCounters(updatedDictionaryCounters)
                .build();
    }

    private QueryData getUpdatedQueryData(TableType tableType, QueryData queryData, String updatedName)
    {
        List<Object> expectedResult = new ArrayList<>(queryData.expected_result());
        Map<String, Long> expectedCounters = queryData.expected_counters();
        String updatedQuery = getTableName();

        if (updatedName != null && !updatedName.isEmpty()) {
            updatedQuery = queryData.query().replace(getTableName(), updatedName);
        }
        if (tableType == TableType.warp_delta_lake) {
            if (queryData.expected_dl_result() != null) {
                expectedResult = new ArrayList<>(queryData.expected_dl_result());
            }
            if (queryData.dl_expected_counters() != null) {
                expectedCounters = queryData.dl_expected_counters();
            }
        }
        else if (tableType == TableType.warp_iceberg) {
            if (queryData.expected_iceberg_result() != null) {
                expectedResult = new ArrayList<>(queryData.expected_iceberg_result());
            }
            if (queryData.iceberg_expected_counters() != null) {
                expectedCounters = queryData.iceberg_expected_counters();
            }
        }

        return new QueryData(
                updatedQuery,
                expectedResult,
                queryData.query_id,
                expectedCounters,
                queryData.session_properties,
                queryData.skip,
                null,
                null,
                null,
                null,
                queryData.skip_caching);
    }

    public static class Builder
    {
        private String name;
        private int lines;
        private String tableName;
        private List<Column> structure;
        private String dataFormat;
        private List<WarmupRule> warmupRules;
        private Map<String, Object> sessionProperties;
        private String warmQuery;
        private boolean skip;
        private boolean ptEnable = true;
        private boolean skipCaching;
        private List<QueryData> queriesData;
        private WarmTypeForStrings warmTypeForStrings;
        private String description;
        private int expectedWarmFailures;
        List<String> failedWarmupElements;
        private Map<String, Long> expectedDictionaryCounters;
        private Set<TableType> skipType;
        private Map<String, Long> expectedIcebergDictionaryCounters;
        private Map<String, Long> expectedDLDictionaryCounters;
        private int splitCount;
        private List<String> partitionBy;
        private List<Object> bucketedBy;
        private int bucketCount;
        private Optional<String> origTableName = Optional.empty();
        private Map<String, TestFormat> overriding = new HashMap<>();

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder lines(int lines)
        {
            this.lines = lines;
            return this;
        }

        public Builder tableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder structure(List<Column> structure)
        {
            this.structure = structure;
            return this;
        }

        public Builder dataFormat(String dataFormat)
        {
            this.dataFormat = dataFormat;
            return this;
        }

        public Builder warmupRules(List<WarmupRule> warmupRules)
        {
            this.warmupRules = warmupRules;
            return this;
        }

        public Builder sessionProperties(Map<String, Object> sessionProperties)
        {
            this.sessionProperties = sessionProperties;
            return this;
        }

        public Builder warmQuery(String warmQuery)
        {
            this.warmQuery = warmQuery;
            return this;
        }

        public Builder skip(boolean skip)
        {
            this.skip = skip;
            return this;
        }

        public Builder ptEnable(boolean ptEnable)
        {
            this.ptEnable = ptEnable;
            return this;
        }

        public Builder skipCaching(boolean skipCaching)
        {
            this.skipCaching = skipCaching;
            return this;
        }

        public Builder queriesData(List<QueryData> queriesData)
        {
            this.queriesData = queriesData;
            return this;
        }

        public Builder warmTypeForStrings(WarmTypeForStrings warmTypeForStrings)
        {
            this.warmTypeForStrings = warmTypeForStrings;
            return this;
        }

        public Builder description(String description)
        {
            this.description = description;
            return this;
        }

        public Builder expectedWarmFailures(int expectedWarmFailures)
        {
            this.expectedWarmFailures = expectedWarmFailures;
            return this;
        }

        public Builder failedWarmupElements(List<String> failedWarmupElements)
        {
            this.failedWarmupElements = failedWarmupElements;
            return this;
        }

        public Builder expectedDictionaryCounters(Map<String, Long> expectedDictionaryCounters)
        {
            this.expectedDictionaryCounters = expectedDictionaryCounters;
            return this;
        }

        public Builder skipType(Set<TableType> skipType)
        {
            this.skipType = skipType;
            return this;
        }

        public Builder expectedIcebergDictionaryCounters(Map<String, Long> expectedIcebergDictionaryCounters)
        {
            this.expectedIcebergDictionaryCounters = expectedIcebergDictionaryCounters;
            return this;
        }

        public Builder expectedDLDictionaryCounters(Map<String, Long> expectedDLDictionaryCounters)
        {
            this.expectedDLDictionaryCounters = expectedDLDictionaryCounters;
            return this;
        }

        public Builder splitCount(int splitCount)
        {
            this.splitCount = splitCount;
            return this;
        }

        public Builder partitionBy(List<String> partitionBy)
        {
            this.partitionBy = partitionBy;
            return this;
        }

        public Builder bucketedBy(List<Object> bucketedBy)
        {
            this.bucketedBy = bucketedBy;
            return this;
        }

        public Builder bucketCount(int bucketCount)
        {
            this.bucketCount = bucketCount;
            return this;
        }

        public Builder origTableName(Optional<String> origTableName)
        {
            this.origTableName = origTableName;
            return this;
        }

        public Builder overriding(Map<String, TestFormat> overriding)
        {
            this.overriding = overriding;
            return this;
        }

        public TestFormat build()
        {
            return new TestFormat(
                    name,
                    lines,
                    tableName,
                    structure,
                    dataFormat,
                    warmupRules,
                    sessionProperties,
                    warmQuery,
                    skip,
                    ptEnable,
                    skipCaching,
                    queriesData,
                    warmTypeForStrings,
                    description,
                    expectedWarmFailures,
                    failedWarmupElements,
                    expectedDictionaryCounters,
                    skipType,
                    expectedIcebergDictionaryCounters,
                    expectedDLDictionaryCounters,
                    splitCount,
                    partitionBy,
                    bucketedBy,
                    bucketCount,
                    origTableName,
                    overriding);
        }

        public TestFormat build(String overridingKey)
        {
            TestFormat testFormat = new TestFormat(
                    name,
                    lines,
                    tableName,
                    structure,
                    dataFormat,
                    warmupRules,
                    sessionProperties,
                    warmQuery,
                    skip,
                    ptEnable,
                    skipCaching,
                    queriesData,
                    warmTypeForStrings,
                    description,
                    expectedWarmFailures,
                    failedWarmupElements,
                    expectedDictionaryCounters,
                    skipType,
                    expectedIcebergDictionaryCounters,
                    expectedDLDictionaryCounters,
                    splitCount,
                    partitionBy,
                    bucketedBy,
                    bucketCount,
                    origTableName,
                    overriding);
            if (overriding != null) {
                TestFormat overridingTestFormat = overriding.get(overridingKey);
                testFormat = mergeTestFormat(testFormat, overridingTestFormat);
            }
            return testFormat;
        }

        private TestFormat mergeTestFormat(TestFormat baseTestFormat, TestFormat overridingTestFormat)
        {
            if (overridingTestFormat == null) {
                return baseTestFormat;
            }
            String calculatedName = overridingTestFormat.name == null ? baseTestFormat.name : overridingTestFormat.name;
            String calculatedWarmQuery = overridingTestFormat.warm_query == null ? baseTestFormat.warm_query : overridingTestFormat.warm_query;
            int calculatedLines = overridingTestFormat.lines != 0 ? baseTestFormat.lines : overridingTestFormat.lines;
            String calculatedTableName = overridingTestFormat.table_name == null ? baseTestFormat.table_name : overridingTestFormat.table_name;
            String calculatedDataFormat = overridingTestFormat.data_format == null ? baseTestFormat.data_format : overridingTestFormat.data_format;
            Map<String, Object> calculatedSessionProperties = overridingTestFormat.session_properties == null ? baseTestFormat.session_properties : overridingTestFormat.session_properties;
            Map<String, Long> calculatedExpectedDictionaryCounters = overridingTestFormat.expected_dictionary_counters == null ? baseTestFormat.expected_dictionary_counters : overridingTestFormat.expected_dictionary_counters;
            int calculatedExpectedWarmFailures = overridingTestFormat.expected_warm_failures != 0 ? baseTestFormat.expected_warm_failures : overridingTestFormat.expected_warm_failures;
            List<String> calculatedFailedWarmupElements = overridingTestFormat.failed_warmup_elements() != null ? baseTestFormat.failed_warmup_elements() : overridingTestFormat.failed_warmup_elements();
            List<QueryData> queriesData = baseTestFormat.queries_data;
            if (overridingTestFormat.queries_data != null && !overridingTestFormat.queries_data.isEmpty()) {
                queriesData = mergeQueriesWithOverrding(queriesData, overridingTestFormat.queries_data);
            }
            return new TestFormat(
                    calculatedName,
                    calculatedLines,
                    calculatedTableName,
                    structure,
                    calculatedDataFormat,
                    warmupRules,
                    calculatedSessionProperties,
                    calculatedWarmQuery,
                    overridingTestFormat.skip,
                    overridingTestFormat.pt_enable,
                    skipCaching,
                    queriesData,
                    warmTypeForStrings,
                    description,
                    calculatedExpectedWarmFailures,
                    calculatedFailedWarmupElements,
                    calculatedExpectedDictionaryCounters,
                    skipType,
                    expectedIcebergDictionaryCounters,
                    expectedDLDictionaryCounters,
                    splitCount,
                    partitionBy,
                    bucketedBy,
                    bucketCount,
                    origTableName,
                    new HashMap<>());
        }

        private List<QueryData> mergeQueriesWithOverrding(List<QueryData> queriesData, List<QueryData> overridingQueries)
        {
            List<QueryData> ret = new ArrayList<>();
            Map<String, QueryData> overridingQueriesData = overridingQueries.stream().collect(Collectors.toMap(QueryData::query_id, Function.identity()));
            queriesData.forEach(queryData -> {
                QueryData overridingQuery = overridingQueriesData.remove(queryData.query_id);
                if (overridingQuery != null) {
                    ret.add(overridingQuery);
                }
                else {
                    ret.add(queryData);
                }
            });
            ret.addAll(overridingQueriesData.values());
            return ret;
        }
    }
}
