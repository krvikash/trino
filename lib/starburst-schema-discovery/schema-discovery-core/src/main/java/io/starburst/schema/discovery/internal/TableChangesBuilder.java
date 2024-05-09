/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.starburst.schema.discovery.TableChanges;
import io.starburst.schema.discovery.TableChanges.BucketChanges;
import io.starburst.schema.discovery.TableChanges.PartitionValueChanges;
import io.starburst.schema.discovery.TableChanges.TableColumnChanges;
import io.starburst.schema.discovery.TableChanges.TablePathName;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredPartitions.IntegerProjectionMinMaxRange;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.models.SlashEndedPath;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class TableChangesBuilder
{
    private final Errors errors = new Errors();
    private final Map<TablePathName, DiscoveredTable> previousTables = new HashMap<>();
    private final Map<TablePathName, DiscoveredTable> currentTables = new HashMap<>();
    private final SlashEndedPath rootPath;

    public TableChangesBuilder(SlashEndedPath rootPath)
    {
        this.rootPath = requireNonNull(rootPath, "rootPath is null");
    }

    public void addPreviousTable(DiscoveredTable table)
    {
        TablePathName tablePathName = table.extractTablePathName();
        checkArgument(!previousTables.containsKey(tablePathName), "Previous table has already been added: " + tablePathName);
        previousTables.put(tablePathName, table);
    }

    public void addCurrentTable(DiscoveredTable table)
    {
        TablePathName tablePathName = table.extractTablePathName();
        checkArgument(!currentTables.containsKey(tablePathName), "Current table has already been added: " + tablePathName);
        currentTables.put(tablePathName, table);
    }

    public TableChanges build()
    {
        SetView<LowerCaseString> schemaToAdd = Sets.difference(schemaNames(currentTables), schemaNames(previousTables));
        Map<TablePathName, DiscoveredTable> tablesToDrop = difference(previousTables, currentTables);
        Map<TablePathName, DiscoveredTable> tablesToAdd = difference(currentTables, previousTables);

        Set<TablePathName> remainingTables = new HashSet<>(previousTables.keySet());
        remainingTables.retainAll(currentTables.keySet());
        remainingTables = filterConflicting(remainingTables);

        Map<TablePathName, DiscoveredTable> tablesToRecreateForProjectionChanges = buildProjectedPartitionChanges(remainingTables);
        remainingTables = Sets.difference(remainingTables, tablesToRecreateForProjectionChanges.keySet());

        Map<TablePathName, BucketChanges> bucketChanges = buildBucketChanges(remainingTables);
        Map<TablePathName, TableColumnChanges> columnChanges = buildColumnChanges(remainingTables);
        Map<TablePathName, TableColumnChanges> partitionColumnChanges = buildPartitionColumnChangesForNonProjected(remainingTables);
        Map<TablePathName, PartitionValueChanges> partitionValueChanges = buildPartitionValueChangesForNonProjected(remainingTables);

        return new TableChanges(
                rootPath,
                schemaToAdd,
                tablesToDrop.values().stream().filter(DiscoveredTable::valid).collect(toImmutableList()),
                ImmutableList.copyOf(tablesToAdd.values()),
                ImmutableList.copyOf(tablesToRecreateForProjectionChanges.values()),
                columnChanges,
                partitionColumnChanges,
                partitionValueChanges,
                bucketChanges,
                errors.buildPathErrors());
    }

    private Set<LowerCaseString> schemaNames(Map<TablePathName, DiscoveredTable> tables)
    {
        return tables.values().stream().flatMap(table -> table.tableName().schemaName().stream()).collect(toImmutableSet());
    }

    private Map<TablePathName, PartitionValueChanges> buildPartitionValueChangesForNonProjected(Collection<TablePathName> remainingTables)
    {
        return remainingTables.stream()
                .filter(tablePath -> !currentTables.get(tablePath).hasAnyProjectedPartition())
                .flatMap(tablePath -> {
                    DiscoveredPartitions previousPartitions = previousTables.get(tablePath).discoveredPartitions();
                    DiscoveredPartitions currentPartitions = currentTables.get(tablePath).discoveredPartitions();
                    ImmutableSet<DiscoveredPartitionValues> previousPartitionValues = ImmutableSet.copyOf(previousPartitions.values());
                    ImmutableSet<DiscoveredPartitionValues> currentPartitionValues = ImmutableSet.copyOf(currentPartitions.values());
                    Set<DiscoveredPartitionValues> droppedPartitionValues = Sets.difference(previousPartitionValues, currentPartitionValues);
                    Set<DiscoveredPartitionValues> addedPartitionValues = Sets.difference(currentPartitionValues, previousPartitionValues);
                    if (droppedPartitionValues.isEmpty() && addedPartitionValues.isEmpty()) {
                        return Optional.<Entry<TablePathName, PartitionValueChanges>>empty().stream();
                    }
                    SimpleEntry<TablePathName, PartitionValueChanges> entry = new SimpleEntry<>(tablePath, new PartitionValueChanges(previousPartitions.columns(), currentPartitions.columns(), ImmutableList.copyOf(droppedPartitionValues), ImmutableList.copyOf(addedPartitionValues)));
                    return Optional.of(entry).stream();
                })
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    private Map<TablePathName, BucketChanges> buildBucketChanges(Collection<TablePathName> remainingTables)
    {
        return remainingTables.stream()
                .flatMap(tablePath -> {
                    ImmutableSet<LowerCaseString> previousBuckets = ImmutableSet.copyOf(previousTables.get(tablePath).buckets());
                    ImmutableSet<LowerCaseString> currentBuckets = ImmutableSet.copyOf(currentTables.get(tablePath).buckets());
                    Sets.SetView<LowerCaseString> droppedBuckets = Sets.difference(previousBuckets, currentBuckets);
                    Sets.SetView<LowerCaseString> addedBuckets = Sets.difference(currentBuckets, previousBuckets);
                    if (droppedBuckets.isEmpty() && addedBuckets.isEmpty()) {
                        return Optional.<Map.Entry<TablePathName, BucketChanges>>empty().stream();
                    }
                    AbstractMap.SimpleEntry<TablePathName, BucketChanges> entry = new AbstractMap.SimpleEntry<>(tablePath, new BucketChanges(droppedBuckets, addedBuckets));
                    return Optional.of(entry).stream();
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<TablePathName, DiscoveredTable> buildProjectedPartitionChanges(Collection<TablePathName> remainingTables)
    {
        return remainingTables.stream()
                .filter(tablePath -> currentTables.get(tablePath).hasAnyProjectedPartition())
                .flatMap(tablePath -> {
                    DiscoveredPartitions previousPartitions = previousTables.get(tablePath).discoveredPartitions();
                    DiscoveredPartitions currentPartitions = currentTables.get(tablePath).discoveredPartitions();

                    if (!previousPartitions.columns().equals(currentPartitions.columns()) ||
                            !previousPartitions.columnProjections().equals(currentPartitions.columnProjections())) {
                        return Optional.of(getTableAsEntry(tablePath)).stream();
                    }

                    boolean areAllProjectionsInMatch = currentPartitions.columns().stream()
                            .allMatch(column -> {
                                InferredPartitionProjection previousProjection = previousPartitions.columnProjections().get(column.name());
                                InferredPartitionProjection currentProjection = currentPartitions.columnProjections().get(column.name());
                                if (!currentProjection.equals(previousProjection)) {
                                    return false;
                                }
                                return switch (currentProjection.projectionType()) {
                                    case INTEGER -> {
                                        IntegerProjectionMinMaxRange previousIntegerProjection = currentPartitions.computeIntegerProjectionRange(column.name());
                                        IntegerProjectionMinMaxRange currentIntegerProjection = previousPartitions.computeIntegerProjectionRange(column.name());
                                        yield currentIntegerProjection.equals(previousIntegerProjection);
                                    }
                                    case ENUM -> {
                                        String previousEnumProjection = currentPartitions.computeEnumProjectionPossibleValues(column.name());
                                        String currentEnumProjection = previousPartitions.computeEnumProjectionPossibleValues(column.name());
                                        yield currentEnumProjection.equals(previousEnumProjection);
                                    }
                                    case DATE -> throw new RuntimeException("Date partition projection type is not supported");
                                    case INJECTED -> true;
                                };
                            });
                    if (!areAllProjectionsInMatch) {
                        return Optional.of(getTableAsEntry(tablePath)).stream();
                    }

                    return Optional.<Map.Entry<TablePathName, DiscoveredTable>>empty().stream();
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private AbstractMap.SimpleEntry<TablePathName, DiscoveredTable> getTableAsEntry(TablePathName tablePathName)
    {
        return new AbstractMap.SimpleEntry<>(tablePathName, currentTables.get(tablePathName));
    }

    // even if table names are equal, we should consider validity, to be able to heal tables
    // which were errored previously, but now are fixed
    private Map<TablePathName, DiscoveredTable> difference(Map<TablePathName, DiscoveredTable> t1, Map<TablePathName, DiscoveredTable> t2)
    {
        Map<TablePathNameAndValidity, DiscoveredTable> t1IdentifierMap = t1.entrySet()
                .stream()
                .collect(toImmutableMap(e -> new TablePathNameAndValidity(e.getKey(), e.getValue().valid()), Entry::getValue));
        Map<TablePathNameAndValidity, DiscoveredTable> t2IdentifierMap = t2.entrySet()
                .stream()
                .collect(toImmutableMap(e -> new TablePathNameAndValidity(e.getKey(), e.getValue().valid()), Entry::getValue));

        Map<TablePathNameAndValidity, DiscoveredTable> temp = new HashMap<>(t1IdentifierMap);
        temp.keySet().removeAll(t2IdentifierMap.keySet());

        return temp.entrySet().stream().collect(toImmutableMap(e -> e.getKey().tablePathName(), Entry::getValue));
    }

    private Map<TablePathName, TableColumnChanges> buildColumnChanges(Collection<TablePathName> remainingTables)
    {
        return remainingTables.stream()
                .collect(toImmutableMap(identity(), tableName -> columnChangesForTable(tableName, discoveredTable -> discoveredTable.columns().columns())));
    }

    private Map<TablePathName, TableColumnChanges> buildPartitionColumnChangesForNonProjected(Collection<TablePathName> remainingTables)
    {
        return remainingTables.stream()
                .filter(tableName -> !currentTables.get(tableName).hasAnyProjectedPartition())
                .collect(toImmutableMap(identity(), tableName -> columnChangesForTable(tableName, discoveredTable -> discoveredTable.discoveredPartitions().columns())));
    }

    private TableColumnChanges columnChangesForTable(TablePathName tablePathName, Function<DiscoveredTable, List<Column>> accessor)
    {
        ImmutableList.Builder<Column> columnsToAdd = ImmutableList.builder();
        ImmutableSet.Builder<LowerCaseString> columnsToDrop = ImmutableSet.builder();
        ImmutableList.Builder<TableChanges.ColumnRename> columnRenames = ImmutableList.builder();
        List<Column> previousColumns = accessor.apply(previousTables.get(tablePathName));
        List<Column> currentColumns = accessor.apply(currentTables.get(tablePathName));
        for (int i = 0; i < Math.max(previousColumns.size(), currentColumns.size()); ++i) {
            Column previousColumn = (i < previousColumns.size()) ? previousColumns.get(i) : null;
            Column currentColumn = (i < currentColumns.size()) ? currentColumns.get(i) : null;

            if (previousColumn == null) {
                if (currentColumn != null) {
                    columnsToAdd.add(currentColumn);
                }
            }
            else if (currentColumn == null) {
                columnsToDrop.add(previousColumn.name());
            }
            else {
                boolean hasSameType = previousColumn.type().equals(currentColumn.type());
                boolean hasSameName = previousColumn.name().equals(currentColumn.name());
                if (!hasSameType) {
                    columnsToDrop.add(previousColumn.name());
                    columnsToAdd.add(currentColumn);
                }
                else if (!hasSameName) {
                    columnRenames.add(new TableChanges.ColumnRename(previousColumn.name(), currentColumn.name()));
                }
            }
        }
        return new TableColumnChanges(columnRenames.build(), columnsToAdd.build(), columnsToDrop.build());
    }

    private String lcase(String s)
    {
        return s.toLowerCase(Locale.getDefault());
    }

    private Set<TablePathName> filterConflicting(Collection<TablePathName> remainingTables)
    {
        return remainingTables.stream()
                .filter(this::hasNoConflicts)
                .collect(toImmutableSet());
    }

    private boolean hasNoConflicts(TablePathName tablePathName)
    {
        DiscoveredTable previousTable = previousTables.get(tablePathName);
        DiscoveredTable currentTable = currentTables.get(tablePathName);
        boolean hasNoConflicts = true;

        if (!previousTable.valid()) {
            errors.addTableError(currentTable.path(), "Previous table [%s] is invalid and will be ignored.", previousTable.tableName());
            hasNoConflicts = false;
        }
        if (!currentTable.valid()) {
            errors.addTableError(currentTable.path(), "Current table [%s] is invalid and will be ignored.", currentTable.tableName());
            hasNoConflicts = false;
        }
        if (!previousTable.format().equals(currentTable.format())) {
            errors.addTableError(currentTable.path(), "Format change in table [%s]. Previous: [%s] Current: [%s]. Table will be ignored.", tablePathName, previousTable.format(), currentTable.format());
            hasNoConflicts = false;
        }
        if (!previousTable.path().equals(currentTable.path())) {
            errors.addTableError(currentTable.path(), "Path change in table [%s]. Previous: [%s] Current: [%s]. Table will be ignored.", tablePathName, previousTable.path(), currentTable.path());
            hasNoConflicts = false;
        }

        return hasNoConflicts;
    }

    private record TablePathNameAndValidity(TablePathName tablePathName, boolean isValid)
    {
        public TablePathNameAndValidity
        {
            requireNonNull(tablePathName, "tablePathName is null");
        }
    }
}
