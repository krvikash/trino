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

import java.time.Duration;
import java.util.List;
import java.util.Map;

public record TestCasesFormat(
        String name,
        int lines,
        String table_name,
        List<TestFormat.Column> structure,
        List<TestCase> cases,
        boolean skip,
        boolean pt_enable,
        List<String> partition_by,
        List<Object> bucketed_by,
        int bucket_count)
{
    public String getTableName()
    {
        return table_name() != null ? table_name() : name();
    }

    public record TestCase(
            String case_info,
            List<WarmupRule> warmup_rules,
            boolean default_warmup,
            String warm_query,
            boolean manual_demoter,
            int sleep_time_for_ttl,
            Map<String, Object> expected_result,
            List<TestFormat.QueryData> queries_data,
            int expected_warm_failures,
            List<String> failed_warmup_elements,
            boolean skip) {}

    public record WarmupRule(
            String colNameId,
            List<WarmupPredicateRule> predicates,
            WarmUpType warmUpType,
            int priority,
            Duration ttl) {}
}
