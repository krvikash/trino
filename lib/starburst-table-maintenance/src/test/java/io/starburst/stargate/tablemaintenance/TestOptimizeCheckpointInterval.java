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
package io.starburst.stargate.tablemaintenance;

import io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.DailyCheckpointInterval;
import io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.FixedCheckpointInterval;
import io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.MonthlyCheckpointInterval;
import io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.NoCheckpointInterval;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.buildIcebergFileModifiedTimePredicate;
import static io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.createNextIntervalForLatestCheckpoint;
import static io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.extractOptimizeCheckpointFromQuery;
import static io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.generateSingleBetweenPredicate;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOptimizeCheckpointInterval
{
    @Test
    public void testCheckpointsNextIntervalTransformation()
    {
        LocalDateTime testCheckpoint = LocalDateTime.of(2025, 7, 20, 0, 0, 0);

        assertThat(createNextIntervalForLatestCheckpoint(new NoCheckpointInterval(), Optional.of(testCheckpoint), testCheckpoint.plusMonths(2)))
                .isEqualTo(new MonthlyCheckpointInterval());
        assertThat(createNextIntervalForLatestCheckpoint(new NoCheckpointInterval(), Optional.empty(), testCheckpoint.plusDays(5)))
                .isEqualTo(new MonthlyCheckpointInterval());

        assertThat(createNextIntervalForLatestCheckpoint(new FixedCheckpointInterval(), Optional.of(testCheckpoint), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(5));
        assertThat(createNextIntervalForLatestCheckpoint(new FixedCheckpointInterval(), Optional.of(testCheckpoint), testCheckpoint.plusMonths(2)))
                .isEqualTo(new MonthlyCheckpointInterval());
        assertThat(createNextIntervalForLatestCheckpoint(new FixedCheckpointInterval(), Optional.empty(), testCheckpoint.plusDays(5)))
                .isEqualTo(new MonthlyCheckpointInterval());

        assertThat(createNextIntervalForLatestCheckpoint(new MonthlyCheckpointInterval(), Optional.of(testCheckpoint), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(28));
        assertThat(createNextIntervalForLatestCheckpoint(new MonthlyCheckpointInterval(), Optional.of(testCheckpoint), testCheckpoint.plusMonths(2)))
                .isEqualTo(new DailyCheckpointInterval(28));
        assertThat(createNextIntervalForLatestCheckpoint(new MonthlyCheckpointInterval(), Optional.empty(), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(28));

        assertThat(createNextIntervalForLatestCheckpoint(new DailyCheckpointInterval(20), Optional.of(testCheckpoint), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(19));
        assertThat(createNextIntervalForLatestCheckpoint(new DailyCheckpointInterval(3), Optional.of(testCheckpoint), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(2));
        assertThat(createNextIntervalForLatestCheckpoint(new DailyCheckpointInterval(20), Optional.of(testCheckpoint), testCheckpoint.plusMonths(2)))
                .isEqualTo(new DailyCheckpointInterval(19));
        assertThat(createNextIntervalForLatestCheckpoint(new DailyCheckpointInterval(20), Optional.empty(), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(19));

        assertThat(createNextIntervalForLatestCheckpoint(new DailyCheckpointInterval(1), Optional.of(testCheckpoint), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(1));
        assertThat(createNextIntervalForLatestCheckpoint(new DailyCheckpointInterval(1), Optional.of(testCheckpoint), testCheckpoint.plusMonths(2)))
                .isEqualTo(new DailyCheckpointInterval(1));
        assertThat(createNextIntervalForLatestCheckpoint(new DailyCheckpointInterval(1), Optional.empty(), testCheckpoint.plusDays(5)))
                .isEqualTo(new DailyCheckpointInterval(1));
    }

    @Test
    public void testCheckpointDatesCalculation()
    {
        LocalDateTime testEndDateTime = LocalDateTime.of(2025, 7, 28, 0, 0, 0);
        LocalDateTime yearBack = testEndDateTime.minusDays(365);
        {
            OptimizeCheckpointInterval noCheckpointInterval = new NoCheckpointInterval();
            assertThat(noCheckpointInterval.generateInitialOptimizeCheckpointPredicates(testEndDateTime)).isEmpty();
            assertThat(noCheckpointInterval.generateRemainingOptimizeCheckpointPredicates(testEndDateTime.minusDays(5), testEndDateTime))
                    .isEmpty();
        }
        {
            OptimizeCheckpointInterval monthlyCheckpointInterval = new MonthlyCheckpointInterval();
            assertThat(monthlyCheckpointInterval.generateInitialOptimizeCheckpointPredicates(testEndDateTime))
                    .hasSize(32)
                    .containsExactly(
                            "WHERE \"$file_modified_time\" < from_iso8601_timestamp('2023-01-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-01-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-02-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-02-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-03-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-03-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-04-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-04-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-05-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-05-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-06-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-06-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-07-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-07-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-08-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-08-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-09-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-09-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-10-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-10-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-11-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-11-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-12-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-12-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-01-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-01-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-02-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-02-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-03-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-03-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-04-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-04-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-05-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-05-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-06-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-06-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-07-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-28T00:00:00Z')");

            // months since 2024-07-28 to 2025-07-28, plus last period
            assertThat(monthlyCheckpointInterval.generateRemainingOptimizeCheckpointPredicates(yearBack, testEndDateTime))
                    .hasSize(12)
                    .containsExactly(
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-28T00:00:00Z')");
        }
        {
            OptimizeCheckpointInterval dailyCheckpointInterval = new DailyCheckpointInterval(7);
            assertThat(dailyCheckpointInterval.generateInitialOptimizeCheckpointPredicates(testEndDateTime))
                    .hasSize(136)
                    .containsExactly(
                            "WHERE \"$file_modified_time\" < from_iso8601_timestamp('2023-01-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-01-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-01-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-01-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-01-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-01-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-01-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-01-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-01-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-01-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-02-05T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-02-05T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-02-12T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-02-12T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-02-19T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-02-19T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-02-26T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-02-26T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-03-05T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-03-05T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-03-12T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-03-12T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-03-19T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-03-19T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-03-26T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-03-26T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-04-02T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-04-02T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-04-09T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-04-09T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-04-16T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-04-16T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-04-23T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-04-23T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-04-30T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-04-30T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-05-07T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-05-07T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-05-14T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-05-14T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-05-21T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-05-21T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-05-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-05-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-06-04T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-06-04T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-06-11T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-06-11T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-06-18T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-06-18T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-06-25T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-06-25T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-07-02T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-07-02T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-07-09T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-07-09T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-07-16T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-07-16T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-07-23T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-07-23T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-07-30T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-07-30T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-08-06T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-08-06T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-08-13T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-08-13T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-08-20T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-08-20T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-08-27T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-08-27T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-09-03T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-09-03T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-09-10T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-09-10T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-09-17T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-09-17T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-09-24T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-09-24T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-10-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-10-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-10-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-10-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-10-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-10-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-10-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-10-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-10-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-10-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-11-05T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-11-05T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-11-12T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-11-12T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-11-19T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-11-19T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-11-26T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-11-26T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-12-03T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-12-03T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-12-10T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-12-10T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-12-17T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-12-17T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-12-24T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-12-24T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-12-31T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-12-31T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-01-07T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-01-07T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-01-14T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-01-14T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-01-21T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-01-21T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-01-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-01-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-02-04T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-02-04T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-02-11T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-02-11T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-02-18T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-02-18T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-02-25T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-02-25T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-03-03T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-03-03T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-03-10T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-03-10T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-03-17T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-03-17T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-03-24T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-03-24T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-03-31T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-03-31T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-04-07T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-04-07T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-04-14T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-04-14T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-04-21T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-04-21T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-04-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-04-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-05-05T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-05-05T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-05-12T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-05-12T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-05-19T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-05-19T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-05-26T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-05-26T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-06-02T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-06-02T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-06-09T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-06-09T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-06-16T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-06-16T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-06-23T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-06-23T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-06-30T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-06-30T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-07-07T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-07T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-07-14T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-14T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-07-21T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-21T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-07-28T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-04T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-04T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-11T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-11T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-18T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-18T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-25T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-25T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-06T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-06T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-13T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-13T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-20T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-20T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-27T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-27T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-03T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-03T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-10T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-10T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-17T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-17T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-24T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-24T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-05T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-05T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-12T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-12T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-19T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-19T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-26T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-26T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-02T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-02T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-09T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-09T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-16T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-16T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-23T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-23T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-02T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-02T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-09T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-09T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-16T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-16T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-23T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-23T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-30T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-30T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-06T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-06T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-13T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-13T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-20T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-20T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-27T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-27T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-04T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-04T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-11T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-11T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-18T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-18T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-25T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-25T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-06T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-06T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-13T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-13T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-20T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-20T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-27T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-27T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-28T00:00:00Z')");

            // weeks since 2024-07-28 to 2025-07-28, plus last period
            assertThat(dailyCheckpointInterval.generateRemainingOptimizeCheckpointPredicates(yearBack, testEndDateTime))
                    .hasSize(53)
                    .containsExactly(
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-04T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-04T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-11T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-11T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-18T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-18T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-08-25T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-08-25T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-09-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-09-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-06T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-06T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-13T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-13T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-20T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-20T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-10-27T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-10-27T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-03T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-03T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-10T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-10T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-17T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-17T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-11-24T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-11-24T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2024-12-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-12-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-05T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-05T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-12T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-12T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-19T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-19T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-01-26T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-01-26T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-02T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-02T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-09T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-09T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-16T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-16T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-02-23T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-02-23T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-02T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-02T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-09T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-09T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-16T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-16T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-23T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-23T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-03-30T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-03-30T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-06T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-06T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-13T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-13T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-20T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-20T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-04-27T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-04-27T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-04T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-04T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-11T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-11T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-18T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-18T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-05-25T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-05-25T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-01T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-01T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-08T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-08T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-15T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-15T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-22T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-22T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-06-29T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-06-29T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-06T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-06T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-13T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-13T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-20T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-20T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-27T00:00:00Z')",
                            "WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2025-07-27T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-28T00:00:00Z')");
        }
        {
            FixedCheckpointInterval fixedCheckpointInterval = new FixedCheckpointInterval();
            assertThat(fixedCheckpointInterval.generateInitialOptimizeCheckpointPredicates(testEndDateTime))
                    .containsExactly(
                            "WHERE \"$file_modified_time\" < from_iso8601_timestamp('2025-07-28T00:00:00Z')");
            assertThat(fixedCheckpointInterval.generateRemainingOptimizeCheckpointPredicates(yearBack, testEndDateTime))
                    .containsExactly("WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2024-07-28T00:00:00Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2025-07-28T00:00:00Z')");
        }
    }

    @Test
    public void testBuildingPredicateTruncatesToSeconds()
    {
        LocalDateTime testTime = LocalDateTime.of(2023, 5, 5, 5, 5, 5, 5);
        String predicate = buildIcebergFileModifiedTimePredicate("<", testTime);
        Optional<LocalDateTime> localDateTime = extractOptimizeCheckpointFromQuery("ALTER TABLE x.y.z EXECUTE OPTIMIZE WHERE " + predicate);
        assertThat(localDateTime).hasValue(testTime.truncatedTo(ChronoUnit.SECONDS));
    }

    @Test
    public void testCreatingSingleBetweenPredicate()
    {
        LocalDateTime testTime = LocalDateTime.of(2023, 5, 5, 5, 5, 5, 5);
        assertThat(generateSingleBetweenPredicate(testTime, testTime.plusSeconds(5)))
                .isEqualTo("WHERE \"$file_modified_time\" >= from_iso8601_timestamp('2023-05-05T05:05:05Z') AND \"$file_modified_time\" < from_iso8601_timestamp('2023-05-05T05:05:10Z')");
    }
}
