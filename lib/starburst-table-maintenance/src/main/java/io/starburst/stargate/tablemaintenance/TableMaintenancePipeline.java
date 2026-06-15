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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public record TableMaintenancePipeline(
        MaintenanceQueries optimizeQueries,
        MaintenanceQueries analyzeQueries,
        MaintenanceQueries expireSnapshotsQueries,
        MaintenanceQueries vacuumQueries,
        MaintenanceTrigger maintenanceTrigger,
        TableOptimizeStatus tableOptimizeStatus,
        OptimizeManifestsMode optimizeManifestsMode,
        RemoveDanglingDeleteFilesMode removeDanglingDeleteFilesMode,
        OptimizePositionDeletesMode optimizePositionDeletesMode)
{
    public TableMaintenancePipeline
    {
        requireNonNull(optimizeQueries, "optimizeQueries is null");
        requireNonNull(analyzeQueries, "analyzeQueries is null");
        requireNonNull(expireSnapshotsQueries, "expireSnapshotsQueries is null");
        requireNonNull(vacuumQueries, "vacuumQueries is null");
        checkState(optimizeQueries.asValidMaintenanceOperationQueries().isPresent() ||
                        analyzeQueries.asValidMaintenanceOperationQueries().isPresent() ||
                        expireSnapshotsQueries.asValidMaintenanceOperationQueries().isPresent() ||
                        vacuumQueries.asValidMaintenanceOperationQueries().isPresent(),
                "Maintenance pipeline requires at least one query");
        requireNonNull(maintenanceTrigger, "maintenanceTrigger is null");
        requireNonNull(tableOptimizeStatus, "tableOptimizeStatus is null");
        requireNonNull(optimizeManifestsMode, "optimizeManifestsMode is null");
        requireNonNull(removeDanglingDeleteFilesMode, "removeDanglingDeleteFilesMode is null");
        requireNonNull(optimizePositionDeletesMode, "optimizePositionDeletesMode is null");
    }

    public Optional<MaintenanceQueries> nextOperationQueries(Optional<MaintenanceQueries> previousOperationQueriesMaybe)
    {
        if (previousOperationQueriesMaybe.isEmpty()) {
            return Optional.of(firstOperationQueries());
        }
        return previousOperationQueriesMaybe.map(MaintenanceQueries::maintenanceOperation)
                .flatMap(previousOperation -> {
                    if (previousOperation == MaintenanceOperation.OPTIMIZE) {
                        return analyzeQueries.asValidMaintenanceOperationQueries()
                                .or(expireSnapshotsQueries::asValidMaintenanceOperationQueries)
                                .or(vacuumQueries::asValidMaintenanceOperationQueries);
                    }

                    if (previousOperation == MaintenanceOperation.ANALYZE) {
                        return expireSnapshotsQueries.asValidMaintenanceOperationQueries()
                                .or(vacuumQueries::asValidMaintenanceOperationQueries);
                    }

                    if (previousOperation == MaintenanceOperation.EXPIRE_SNAPSHOTS) {
                        return vacuumQueries.asValidMaintenanceOperationQueries();
                    }

                    return Optional.empty();
                });
    }

    public Map<MaintenanceOperation, Integer> asMaintenanceQueriesCounts()
    {
        return Maps.transformValues(
                asMaintenanceQueriesMap(),
                MaintenanceQueries::totalQueryCount);
    }

    public Map<MaintenanceOperation, MaintenanceQueries> asMaintenanceQueriesMap()
    {
        ImmutableMap.Builder<MaintenanceOperation, MaintenanceQueries> builder = ImmutableMap.builder();
        optimizeQueries.asValidMaintenanceOperationQueries().ifPresent(mq -> builder.put(MaintenanceOperation.OPTIMIZE, mq));
        analyzeQueries.asValidMaintenanceOperationQueries().ifPresent(mq -> builder.put(MaintenanceOperation.ANALYZE, mq));
        expireSnapshotsQueries.asValidMaintenanceOperationQueries().ifPresent(mq -> builder.put(MaintenanceOperation.EXPIRE_SNAPSHOTS, mq));
        vacuumQueries.asValidMaintenanceOperationQueries().ifPresent(mq -> builder.put(MaintenanceOperation.VACUUM, mq));
        return builder.buildKeepingLast();
    }

    public MaintenanceOperation firstOperation()
    {
        return optimizeQueries.asValidMaintenanceOperationQueries()
                .or(analyzeQueries::asValidMaintenanceOperationQueries)
                .or(expireSnapshotsQueries::asValidMaintenanceOperationQueries)
                .map(MaintenanceQueries::maintenanceOperation)
                .orElse(MaintenanceOperation.VACUUM);
    }

    private MaintenanceQueries firstOperationQueries()
    {
        return optimizeQueries.asValidMaintenanceOperationQueries()
                .or(analyzeQueries::asValidMaintenanceOperationQueries)
                .or(expireSnapshotsQueries::asValidMaintenanceOperationQueries)
                .orElse(vacuumQueries);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        MaintenanceQueries optimizeQueries = MaintenanceQueries.EMPTY;
        OptimizeQueryIterator optimizeQueryIterator;
        MaintenanceQueries analyzeQueries = MaintenanceQueries.EMPTY;
        MaintenanceQueries expireSnapshotsQueries = MaintenanceQueries.EMPTY;
        MaintenanceQueries vacuumQueries = MaintenanceQueries.EMPTY;
        MaintenanceTrigger maintenanceTrigger;
        TableOptimizeStatus tableOptimizeStatus = FileModifiedTimeBasedOptimizeStatus.DEFAULT;
        OptimizeManifestsMode optimizeManifestsMode = OptimizeManifestsMode.EXCLUDE_OPTIMIZE_MANIFESTS;
        RemoveDanglingDeleteFilesMode removeDanglingDeleteFilesMode = RemoveDanglingDeleteFilesMode.EXCLUDE_REMOVE_DANGLING_DELETE_FILES;
        OptimizePositionDeletesMode optimizePositionDeletesMode = OptimizePositionDeletesMode.EXCLUDE_OPTIMIZE_POSITION_DELETES;

        private Builder() {}

        public void withOptimizeQueries(List<String> optimizeQueries)
        {
            checkState(optimizeQueryIterator == null, "withOptimizeQueries and withOptimizeQueryIterator are mutually exclusive");
            this.optimizeQueries = new MaintenanceQueries(MaintenanceOperation.OPTIMIZE, optimizeQueries, ImmutableMap.of());
        }

        void withOptimizeQueryIterator(OptimizeQueryIterator queryIterator)
        {
            checkState(optimizeQueries == MaintenanceQueries.EMPTY, "withOptimizeQueries and withOptimizeQueryIterator are mutually exclusive");
            this.optimizeQueryIterator = queryIterator;
        }

        public void withAnalyzeQueries(List<String> analyzeQueries)
        {
            this.analyzeQueries = new MaintenanceQueries(MaintenanceOperation.ANALYZE, analyzeQueries, ImmutableMap.of());
        }

        public void withExpireSnapshotsQueries(List<String> expireSnapshotsQueries, Map<String, String> sessionProperties)
        {
            this.expireSnapshotsQueries = new MaintenanceQueries(MaintenanceOperation.EXPIRE_SNAPSHOTS, expireSnapshotsQueries, sessionProperties);
        }

        public void withVacuumQueries(List<String> vacuumQueries)
        {
            this.vacuumQueries = new MaintenanceQueries(MaintenanceOperation.VACUUM, vacuumQueries, ImmutableMap.of());
        }

        public void withMaintenanceTrigger(MaintenanceTrigger maintenanceTrigger)
        {
            this.maintenanceTrigger = maintenanceTrigger;
        }

        void withTableOptimizeStatus(TableOptimizeStatus tableOptimizeStatus)
        {
            this.tableOptimizeStatus = tableOptimizeStatus;
        }

        public <HistoryType extends HavingStatus, DebugInfoType extends HavingTableOptimizeStatus> MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withOptimizeSettings()
        {
            return new MaintenancePipelineOptimizeSetter<>(this);
        }

        public void withOptimizeManifestsMode(OptimizeManifestsMode optimizeManifestsMode)
        {
            this.optimizeManifestsMode = optimizeManifestsMode;
        }

        public void withRemoveDanglingDeleteFilesMode(RemoveDanglingDeleteFilesMode removeDanglingDeleteFilesMode)
        {
            this.removeDanglingDeleteFilesMode = removeDanglingDeleteFilesMode;
        }

        public boolean isRemoveDanglingDeleteFilesIncludedInOptimize()
        {
            return removeDanglingDeleteFilesMode == RemoveDanglingDeleteFilesMode.INCLUDE_REMOVE_DANGLING_DELETE_FILES_IN_OPTIMIZE;
        }

        public void withOptimizePositionDeletesMode(OptimizePositionDeletesMode optimizePositionDeletesMode)
        {
            this.optimizePositionDeletesMode = optimizePositionDeletesMode;
        }

        public TableMaintenancePipeline build()
        {
            MaintenanceQueries finalOptimizeQueries = optimizeQueryIterator != null
                    ? new MaintenanceQueries(MaintenanceOperation.OPTIMIZE, optimizeQueryIterator, optimizeManifestsMode, removeDanglingDeleteFilesMode, optimizePositionDeletesMode, ImmutableMap.of())
                    : optimizeQueries;
            return new TableMaintenancePipeline(
                    finalOptimizeQueries,
                    analyzeQueries,
                    expireSnapshotsQueries,
                    vacuumQueries,
                    maintenanceTrigger,
                    tableOptimizeStatus,
                    optimizeManifestsMode,
                    removeDanglingDeleteFilesMode,
                    optimizePositionDeletesMode);
        }
    }

    public record MaintenanceQueries(
            MaintenanceOperation maintenanceOperation,
            Iterator<String> queryIterator,
            Map<String, String> sessionProperties,
            int totalQueryCount)
    {
        public static final MaintenanceQueries EMPTY = new MaintenanceQueries(MaintenanceOperation.OPTIMIZE, ImmutableList.of(), ImmutableMap.of());

        private MaintenanceQueries(MaintenanceOperation maintenanceOperation, List<String> queries, Map<String, String> sessionProperties)
        {
            this(maintenanceOperation, ImmutableList.copyOf(queries).iterator(), ImmutableMap.copyOf(sessionProperties), queries.size());
        }

        private MaintenanceQueries(
                MaintenanceOperation maintenanceOperation,
                OptimizeQueryIterator queryIterator,
                OptimizeManifestsMode optimizeManifestsMode,
                RemoveDanglingDeleteFilesMode removeDanglingDeleteFilesMode,
                OptimizePositionDeletesMode optimizePositionDeletesMode,
                Map<String, String> sessionProperties)
        {
            this(maintenanceOperation,
                    queryIterator,
                    ImmutableMap.copyOf(sessionProperties),
                    queryIterator.totalCount() + optimizeManifestsMode.getNumberOfQueries() + removeDanglingDeleteFilesMode.getNumberOfQueries() + optimizePositionDeletesMode.getNumberOfQueries());
        }

        public MaintenanceQueries
        {
            sessionProperties = ImmutableMap.copyOf(sessionProperties);
            requireNonNull(queryIterator, "queryIterator is null");
        }

        public Optional<MaintenanceQueries> asValidMaintenanceOperationQueries()
        {
            return totalQueryCount > 0 ? Optional.of(this) : Optional.empty();
        }
    }
}
