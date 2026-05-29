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
package io.trino.tests;

import io.trino.plugin.hive.HiveQueryRunner;

import java.util.Optional;

public final class TestHiveGpuTpcdsQueries
        extends BaseHiveGpuTpcdsQueriesTest
{
    @Override
    protected void configureRunner(HiveQueryRunner.Builder<?> builder)
    {
        builder.configureGpuLocalExecution();
    }

    @Override
    protected Optional<String> expectedFailure(int queryNumber)
    {
        // TODO remove expectedFailure functionality when all failures gone

        return switch (queryNumber) {
            // TODO (https://starburstdata.atlassian.net/browse/ENG-16539) fix "local exchange requires identity input layout" failure
            case 5 -> Optional.of(
                    """
                    GPU local exchange requires identity input layout (source 0, channel 5, symbol expr::[decimal(7,2)], sourceLayout {ss_store_sk::[bigint]=0, ss_sold_date_sk::[bigint]=1, ss_ext_sales_price::[decimal(7,2)]=2, ss_net_profit::[decimal(7,2)]=3, expr::[decimal(7,2)]=4})""");
            // TODO (https://starburstdata.atlassian.net/browse/ENG-16539) fix "local exchange requires identity input layout" failure
            case 54 -> Optional.of(
                    """
                    GPU local exchange requires identity input layout (source 1, channel 1, symbol ws_bill_customer_sk::[bigint], sourceLayout {ws_sold_date_sk::[bigint]=0, ws_item_sk::[bigint]=1, ws_bill_customer_sk::[bigint]=2})""");
            default -> Optional.empty();
        };
    }

    @Override
    String gpuPlanResource(int queryNumber)
    {
        return "sql/trino/tpcds/hive/gpu/local/q%02d.plan.txt".formatted(queryNumber);
    }
}
