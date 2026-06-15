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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.SIMPLE_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.stargate.tablemaintenance.MaintenanceConstants.ERROR_CODES_TO_REDUCE_OPTIMIZE_CHECKPOINTS;
import static io.starburst.stargate.tablemaintenance.MaintenanceConstants.ERROR_CODE_NAMES_TO_REDUCE_OPTIMIZE_CHECKPOINTS;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.stream.Gatherers.windowSliding;

@JsonTypeInfo(use = SIMPLE_NAME, property = "checkpointKind")
@JsonSubTypes({
        @Type(value = OptimizeCheckpointInterval.DailyCheckpointInterval.class),
        @Type(value = OptimizeCheckpointInterval.MonthlyCheckpointInterval.class),
        @Type(value = OptimizeCheckpointInterval.FixedCheckpointInterval.class),
        @Type(value = OptimizeCheckpointInterval.NoCheckpointInterval.class),
})
public sealed interface OptimizeCheckpointInterval
{
    LocalDateTime INCEPTION_CHECKPOINT = LocalDateTime.ofInstant(Instant.parse("2023-01-01T00:00:00Z"), ZoneOffset.UTC);
    Pattern CHECKPOINT_EXTRACT_PATTERN = Pattern.compile("< from_iso8601_timestamp\\('(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z)'\\)");

    static String generateSingleBetweenPredicate(LocalDateTime start, LocalDateTime end)
    {
        return buildPredicates(ImmutableList.of(start), end, PredicateGenerationMode.AFTER_START_TIME).getFirst();
    }

    static OptimizeCheckpointInterval createNextIntervalForLatestCheckpoint(OptimizeCheckpointInterval currentInterval, Optional<LocalDateTime> latestCheckpoint)
    {
        return createNextIntervalForLatestCheckpoint(currentInterval, latestCheckpoint, LocalDateTime.now(ZoneOffset.UTC));
    }

    @VisibleForTesting
    static OptimizeCheckpointInterval createNextIntervalForLatestCheckpoint(OptimizeCheckpointInterval currentInterval, Optional<LocalDateTime> latestCheckpoint, LocalDateTime now)
    {
        Duration durationSinceLastCheckpoint = computeDurationSinceLastCheckpoint(latestCheckpoint, now);
        // we can't have lower intervals than a day at the moment, so just use fixed interval which can be an arbitrary range
        if (durationSinceLastCheckpoint.toDays() < 1) {
            return new FixedCheckpointInterval();
        }
        if (currentInterval instanceof FixedCheckpointInterval) {
            if (durationSinceLastCheckpoint.toDays() > DailyCheckpointInterval.MAX_DAYS) {
                return new MonthlyCheckpointInterval();
            }
            return new DailyCheckpointInterval(Math.max((int) durationSinceLastCheckpoint.toDays(), DailyCheckpointInterval.MIN_DAYS));
        }

        return currentInterval.nextCheckpointInterval();
    }

    default List<String> generateInitialOptimizeCheckpointPredicates(LocalDateTime endTime)
    {
        return generateRemainingOptimizeCheckpointPredicates(INCEPTION_CHECKPOINT, endTime, PredicateGenerationMode.INCLUDE_BEFORE_START_TIME);
    }

    default List<String> generateRemainingOptimizeCheckpointPredicates(LocalDateTime startTime, LocalDateTime endTime)
    {
        return generateRemainingOptimizeCheckpointPredicates(startTime, endTime, PredicateGenerationMode.AFTER_START_TIME);
    }

    List<String> generateRemainingOptimizeCheckpointPredicates(LocalDateTime startTime, LocalDateTime endTime, PredicateGenerationMode mode);

    OptimizeCheckpointInterval nextCheckpointInterval();

    /*
     * Even though checkpoints can be used for both '<' and '>=' matches,
     * we do care only about < when extracting checkpoint date
     */
    static Optional<LocalDateTime> extractOptimizeCheckpointFromQuery(String queryText)
    {
        if (!(queryText.startsWith("ALTER TABLE") && queryText.contains("EXECUTE OPTIMIZE") && queryText.contains("WHERE") && queryText.contains("\"$file_modified_time\" < from_iso8601_timestamp"))) {
            return Optional.empty();
        }
        // checks against ISO_OFFSET_DATE_TIME at UTC offset, that was previously used in the query
        Matcher matcher = CHECKPOINT_EXTRACT_PATTERN.matcher(queryText);
        if (matcher.find()) {
            return Optional.of(LocalDateTime.parse(matcher.group(1), ISO_OFFSET_DATE_TIME));
        }
        return Optional.empty();
    }

    static String buildIcebergFileModifiedTimePredicate(String operator, LocalDateTime dateTime)
    {
        return "\"$file_modified_time\" %s from_iso8601_timestamp('%s')".formatted(operator, dateTime.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS).format(ISO_OFFSET_DATE_TIME));
    }

    static boolean shouldMakeOptimizeCheckpointsSmaller(Optional<? extends MaintenanceErrorSupplier> maintenanceErrorSupplier)
    {
        return maintenanceErrorSupplier.map(maintenanceError -> {
            boolean isErrorCodeNameValidForCheckpointedOptimize = maintenanceError.errorCodeName().isPresent()
                    && ERROR_CODE_NAMES_TO_REDUCE_OPTIMIZE_CHECKPOINTS.contains(maintenanceError.errorCodeName().get());
            boolean isErrorCodeValidForCheckpointedOptimize = maintenanceError.errorCode().isPresent()
                    && ERROR_CODES_TO_REDUCE_OPTIMIZE_CHECKPOINTS.contains(maintenanceError.errorCode().getAsInt());
            return isErrorCodeNameValidForCheckpointedOptimize || isErrorCodeValidForCheckpointedOptimize;
        }).orElse(false);
    }

    private static List<String> buildPredicates(List<LocalDateTime> checkpoints, LocalDateTime endTime, PredicateGenerationMode mode)
    {
        if (checkpoints.isEmpty()) {
            return ImmutableList.of();
        }
        List<String> intermediateCheckpoints = checkpoints.stream()
                .gather(windowSliding(2))
                // Filter out pairs where both dates are the same, as they do not represent a valid range
                .filter(dates -> !dates.getFirst().isEqual(dates.getLast()))
                .map(dates -> "WHERE %s AND %s".formatted(
                        buildIcebergFileModifiedTimePredicate(">=", dates.getFirst()),
                        buildIcebergFileModifiedTimePredicate("<", dates.getLast())))
                .collect(toImmutableList());
        String lastCheckpoint = "WHERE %s AND %s".formatted(
                buildIcebergFileModifiedTimePredicate(">=", checkpoints.getLast()),
                buildIcebergFileModifiedTimePredicate("<", endTime));

        ImmutableList.Builder<String> predicatesBuilder = ImmutableList.builder();
        if (PredicateGenerationMode.INCLUDE_BEFORE_START_TIME == mode) {
            String initialCheckpoint = "WHERE %s".formatted(buildIcebergFileModifiedTimePredicate("<", checkpoints.getFirst()));
            predicatesBuilder.add(initialCheckpoint);
        }
        return predicatesBuilder
                .addAll(intermediateCheckpoints)
                .add(lastCheckpoint)
                .build();
    }

    record DailyCheckpointInterval(int daysWindow)
            implements OptimizeCheckpointInterval
    {
        private static final Logger log = Logger.get(OptimizeCheckpointInterval.class);
        private static final int MAX_DAYS = 28;
        private static final int MIN_DAYS = 1;

        public DailyCheckpointInterval
        {
            checkArgument(daysWindow >= MIN_DAYS, "daysWindow must be greater than %s".formatted(MIN_DAYS));
        }

        @Override
        public List<String> generateRemainingOptimizeCheckpointPredicates(LocalDateTime startTime, LocalDateTime endTime, PredicateGenerationMode mode)
        {
            checkArgument(startTime.isEqual(INCEPTION_CHECKPOINT) || startTime.isAfter(INCEPTION_CHECKPOINT),
                    "startTime must be equal to or after the inception checkpoint");

            List<LocalDateTime> checkpointDates = startTime.toLocalDate().datesUntil(endTime.toLocalDate(), Period.ofDays(daysWindow))
                    .map(LocalDate::atStartOfDay)
                    .toList();
            return buildPredicates(checkpointDates, endTime, mode);
        }

        @Override
        public OptimizeCheckpointInterval nextCheckpointInterval()
        {
            if (daysWindow == MIN_DAYS) {
                log.warn("Daily checkpoint interval cannot be reduced further, returning DailyCheckpointInterval with 1 day window");
                return this;
            }
            return new DailyCheckpointInterval(daysWindow - DailyCheckpointInterval.MIN_DAYS);
        }
    }

    record MonthlyCheckpointInterval()
            implements OptimizeCheckpointInterval
    {
        @Override
        public List<String> generateRemainingOptimizeCheckpointPredicates(LocalDateTime startTime, LocalDateTime endTime, PredicateGenerationMode mode)
        {
            checkArgument(startTime.isEqual(INCEPTION_CHECKPOINT) || startTime.isAfter(INCEPTION_CHECKPOINT),
                    "startTime must be equal to or after the inception checkpoint");

            List<LocalDateTime> checkpointDates = startTime.toLocalDate().datesUntil(endTime.toLocalDate(), Period.ofMonths(1))
                    .map(LocalDate::atStartOfDay)
                    .toList();
            return buildPredicates(checkpointDates, endTime, mode);
        }

        @Override
        public OptimizeCheckpointInterval nextCheckpointInterval()
        {
            return new DailyCheckpointInterval(DailyCheckpointInterval.MAX_DAYS);
        }
    }

    record FixedCheckpointInterval()
            implements OptimizeCheckpointInterval
    {
        @Override
        public List<String> generateInitialOptimizeCheckpointPredicates(LocalDateTime endTime)
        {
            return ImmutableList.of("WHERE %s".formatted(buildIcebergFileModifiedTimePredicate("<", endTime)));
        }

        @Override
        public List<String> generateRemainingOptimizeCheckpointPredicates(LocalDateTime startTime, LocalDateTime endTime, PredicateGenerationMode mode)
        {
            return buildPredicates(ImmutableList.of(startTime), endTime, mode);
        }

        @Override
        public OptimizeCheckpointInterval nextCheckpointInterval()
        {
            return this;
        }
    }

    // In case of always failing tables, such checkpoint interval might still be in DB.
    @Deprecated
    record NoCheckpointInterval()
            implements OptimizeCheckpointInterval
    {
        @Override
        public List<String> generateRemainingOptimizeCheckpointPredicates(LocalDateTime startTime, LocalDateTime endTime, PredicateGenerationMode mode)
        {
            return ImmutableList.of();
        }

        @Override
        public OptimizeCheckpointInterval nextCheckpointInterval()
        {
            return new MonthlyCheckpointInterval();
        }
    }

    private static Duration computeDurationSinceLastCheckpoint(Optional<LocalDateTime> latestCheckpoint, LocalDateTime until)
    {
        return latestCheckpoint
                .map(checkpoint -> Duration.between(checkpoint, until))
                .orElse(Duration.ofDays(Integer.MAX_VALUE));
    }

    enum PredicateGenerationMode
    {
        INCLUDE_BEFORE_START_TIME, AFTER_START_TIME
    }
}
