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
package io.trino.plugin.warp.warmup;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpErrorCode;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.di.FakeConnectorSessionProvider;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.model.WildcardColumn;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.warmup.model.WarmupPredicateRule;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.plugin.warp.warmup.model.WarmupRuleResult;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

@Singleton
public class WarmupRuleService
{
    private static final Logger logger = Logger.get(WarmupRuleService.class);

    public static final String WARMUP_PATH = "warmup";
    public static final String TASK_NAME_GET = "warmup-rule-get";

    private static final AtomicInteger idGen = new AtomicInteger(1);

    private final Connector proxiedConnector;
    private final StorageEngineConstants storageEngineConstants;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final FakeConnectorSessionProvider fakeConnectorSessionProvider;
    private final GlobalConfig globalConfig;

    private ImmutableMap<Integer, WarmupRule> cache;
    protected final ReadWriteLock readWriteLock;

    @Inject
    public WarmupRuleService(
            @ForWarp Connector proxiedConnector,
            StorageEngineConstants storageEngineConstants,
            WarmupDemoterConfig warmupDemoterConfig,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            FakeConnectorSessionProvider fakeConnectorSessionProvider,
            GlobalConfig globalConfig)
    {
        this.proxiedConnector = requireNonNull(proxiedConnector);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.warmupDemoterConfig = requireNonNull(warmupDemoterConfig);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.fakeConnectorSessionProvider = requireNonNull(fakeConnectorSessionProvider);
        this.globalConfig = requireNonNull(globalConfig);

        ImmutableMap.Builder<Integer, WarmupRule> builder = ImmutableMap.builder();
        cache = builder.buildOrThrow();

        readWriteLock = new ReentrantReadWriteLock();
    }

    public Collection<WarmupRule> getAll()
    {
        readWriteLock.readLock().lock();
        try {
            return cache.values();
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void delete(List<Integer> ids)
    {
        readWriteLock.writeLock().lock();

        try {
            ImmutableMap.Builder<Integer, WarmupRule> builder = ImmutableMap.builder();

            // add the rest
            Map<Integer, WarmupRule> tmpMap = new HashMap<>(cache);
            ids.forEach(tmpMap::remove);
            builder.putAll(tmpMap);
            cache = builder.buildOrThrow();
        }
        catch (Exception e) {
            logger.error("failed to delete new rules ids=%s.", ids);
            throw new TrinoException(
                    WarpErrorCode.WARP_RULE_CONFIGURATION_ERROR,
                    "failed to save new rules config",
                    e);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public WarmupRuleResult save(List<WarmupRule> newWarmupRules)
            throws TrinoException
    {
        readWriteLock.writeLock().lock();
        try {
            WarmupRuleResult warmupRuleResult = validate(getAll(), newWarmupRules);
            List<WarmupRule> appliedRules = List.of();
            if (!warmupRuleResult.appliedRules().isEmpty()) {
                appliedRules = internalSave(warmupRuleResult.appliedRules(), false);
            }
            return new WarmupRuleResult(appliedRules, warmupRuleResult.rejectedRules());
        }
        catch (Exception e) {
            logger.error(e, "failed to save new warmupRules=%s", newWarmupRules);
            throw new TrinoException(
                    WarpErrorCode.WARP_RULE_CONFIGURATION_ERROR,
                    "failed to save new warmupRules",
                    e);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public WarmupRuleResult replaceAll(List<WarmupRule> newWarmupRules)
            throws TrinoException
    {
        WarmupRuleResult warmupRuleResult = validate(Collections.emptyList(), newWarmupRules);

        readWriteLock.writeLock().lock();
        try {
            List<WarmupRule> appliedRules = List.of();
            if (!warmupRuleResult.appliedRules().isEmpty() || newWarmupRules.isEmpty()) {
                appliedRules = internalSave(warmupRuleResult.appliedRules(), true);
            }
            return new WarmupRuleResult(appliedRules, warmupRuleResult.rejectedRules());
        }
        catch (Exception e) {
            String error = String.format(Locale.US, "failed to replace existing rules with new rules=%s", newWarmupRules);
            throw new TrinoException(WarpErrorCode.WARP_RULE_CONFIGURATION_ERROR, error, e);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public WarmupRuleResult validate(Collection<WarmupRule> existingWarmupRules, Collection<WarmupRule> newWarmupRules)
    {
        WarmupRuleResult warmupRuleResult = validateNewRules(existingWarmupRules, newWarmupRules);
        if (!warmupRuleResult.rejectedRules().isEmpty()) {
            logger.info(warmupRuleResult.rejectedRules().toString());
        }
        return warmupRuleResult;
    }

    private WarmupRuleResult validateNewRules(Collection<WarmupRule> existingWarmupRules, Collection<WarmupRule> newWarmupRules)
    {
        Map<WarmupRule, Set<String>> rejectedRules = new HashMap<>();
        List<WarmupRule> appliedRules = new ArrayList<>();

        Map<SchemaTableName, Map<String, ColumnMetadata>> tablesColumnsMetadata = getTableColumnsMetadata(newWarmupRules);

        Map<String, List<WarmupRule>> existingRuleByColumn = existingWarmupRules.stream().collect(groupingBy(this::getColumnUniqueKey));

        Set<Long> existingUniqueRuleIds = existingWarmupRules.stream()
                .map(this::getUniqueRuleId)
                .collect(Collectors.toSet());
        Set<Long> uniqueIds = new HashSet<>();

        Set<Integer> existingRuleIds = existingWarmupRules.stream()
                .map(WarmupRule::getId)
                .collect(Collectors.toSet());
        for (WarmupRule warmupRule : newWarmupRules) {
            long uniqueRuleId = getUniqueRuleId(warmupRule);
            if (uniqueIds.contains(uniqueRuleId) || (isNew(warmupRule) && existingUniqueRuleIds.contains(uniqueRuleId))) {
                String error = String.format(
                        Locale.US,
                        "%d: can't add 2 rules with the same key. rules=%s",
                        WarpErrorCode.WARP_DUPLICATE_RECORD.getCode(),
                        warmupRule);
                rejectedRules.computeIfAbsent(warmupRule, _ -> new HashSet<>()).add(error);
                continue;
            }
            else {
                uniqueIds.add(uniqueRuleId);
            }
            if ((!existingRuleIds.isEmpty() && (warmupRule.getId() != 0)) && !existingRuleIds.contains(warmupRule.getId())) {
                String error = String.format(
                        Locale.US,
                        "%d: New Rule id must be 0, or override an existing rule id",
                        WarpErrorCode.WARP_WARMUP_RULE_ID_NOT_VALID.getCode());
                rejectedRules.computeIfAbsent(warmupRule, _ -> new HashSet<>()).add(error);
                continue;
            }
            Map<String, ColumnMetadata> tableColumnsMetadata = getTableColumnsMetadata(tablesColumnsMetadata, warmupRule);
            if (tableColumnsMetadata == null) {
                String error = String.format(
                        Locale.US,
                        "%d: Rule refer to a non-exist table (%s)",
                        WarpErrorCode.WARP_WARMUP_RULE_UNKNOWN_TABLE.getCode(),
                        getSchemaTableName(warmupRule));
                rejectedRules.computeIfAbsent(warmupRule, _ -> new HashSet<>()).add(error);
            }
            else {
                Optional<Type> columnType = getColumnType(tableColumnsMetadata, warmupRule);
                if (columnType.isEmpty() && !(warmupRule.getWarpColumn() instanceof WildcardColumn)) {
                    String error = String.format(
                            Locale.US,
                            "%d: Rule refer to a non-exist column (%s)",
                            WarpErrorCode.WARP_WARMUP_RULE_UNKNOWN_COLUMN.getCode(),
                            warmupRule.getWarpColumn().getName());
                    rejectedRules.computeIfAbsent(warmupRule, _ -> new HashSet<>()).add(error);
                }
                else {
                    String columnUniqueKey = getColumnUniqueKey(warmupRule);
                    boolean valid = validateRule(rejectedRules, warmupRule, columnType);
                    if (valid) {
                        List<WarmupRule> columnRules = existingRuleByColumn.computeIfAbsent(columnUniqueKey, _ -> new ArrayList<>());
                        columnRules.add(warmupRule); // adding to find failures in the list to be saved
                        appliedRules.add(warmupRule);
                    }
                }
            }
        }
        return new WarmupRuleResult(appliedRules, rejectedRules);
    }

    private boolean validateRule(
            Map<WarmupRule, Set<String>> rejectedRules,
            WarmupRule warmupRule,
            Optional<Type> optionalType)
    {
        Set<String> errors = new HashSet<>();
        if (!(warmupRule.getWarpColumn() instanceof WildcardColumn)) {
            optionalType.ifPresentOrElse(type -> {
                if (warmupRule.getPriority() >= warmupDemoterConfig.getDefaultRulePriority()) {
                    validateTypeIsSupported(errors, warmupRule, type);
                    validateMaxCharLength(errors, warmupRule, type);
                }
                validateDataOnly(errors, warmupRule);
            }, () -> errors.add("WarmUpType is null"));
            if (!errors.isEmpty()) {
                rejectedRules.put(warmupRule, errors);
                return false;
            }
        }
        return true;
    }

    private void validateTypeIsSupported(Set<String> errors, WarmupRule warmupRule, Type type)
    {
        boolean fail = false;
        if (WarmUpType.WARM_UP_TYPE_LUCENE.equals(warmupRule.getWarmUpType())) {
            if (!TypeUtils.isWarmLuceneSupported(type)) {
                fail = true;
            }
        }
        else if (TypeUtils.isRowType(type) || TypeUtils.isMapType(type) ||
                (!WarmUpType.WARM_UP_TYPE_DATA.equals(warmupRule.getWarmUpType()) && (TypeUtils.isArrayType(type) || TypeUtils.isJsonType(type)))) {
            fail = true;
        }
        else if (WarmUpType.WARM_UP_TYPE_DATA.equals(warmupRule.getWarmUpType()) && TypeUtils.isArrayType(type)) {
            ArrayType arrayType = (ArrayType) type;
            if (TypeUtils.isRowType(arrayType.getElementType())) {
                fail = true;
            }
        }

        if (fail) {
            String error = String.format(
                    Locale.US,
                    "%d: Warmup type %s doesn't support column type %s",
                    WarpErrorCode.WARP_WARMUP_RULE_WARMUP_TYPE_DOESNT_SUPPORT_COL_TYPE.getCode(),
                    warmupRule.getWarmUpType(),
                    type);
            errors.add(error);
        }
    }

    private void validateMaxCharLength(Set<String> errors, WarmupRule warmupRule, Type type)
    {
        if (warmupRule.getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA &&
                TypeUtils.isCharType(type) && ((CharType) type).getLength() > storageEngineConstants.getMaxRecLen()) {
            String error = String.format(
                    Locale.US,
                    "%d: Can't warm long Char columns. maximum of %d is allowed",
                    WarpErrorCode.WARP_WARMUP_RULE_ILLEGAL_CHAR_LENGTH.getCode(),
                    storageEngineConstants.getMaxRecLen());
            errors.add(error);
        }
    }

    private void validateDataOnly(Set<String> errors, WarmupRule warmupRule)
    {
        if (globalConfig.isDataOnlyWarming() && !warmupRule.getWarmUpType().equals(WarmUpType.WARM_UP_TYPE_DATA)) {
            errors.add(String.format(
                    Locale.US,
                    "%d: creation of %s warmUpType is not supported with Local Data Storage connector",
                    WarpErrorCode.WARP_INDEX_WARMUP_RULE_IS_NOT_ALLOWED.getCode(),
                    warmupRule.getWarmUpType()));
        }
    }

    private Map<SchemaTableName, Map<String, ColumnMetadata>> getTableColumnsMetadata(Collection<WarmupRule> warmupRules)
    {
        Set<SchemaTableName> schemaTableNames = warmupRules.stream()
                .map(this::getSchemaTableName)
                .collect(Collectors.toSet());
        ConnectorSession session = fakeConnectorSessionProvider.get();
        Pair<ConnectorMetadata, ConnectorTransactionHandle> proxiedMetadata = dispatcherProxiedConnectorTransformer.createProxiedMetadata(proxiedConnector, session);
        ConnectorMetadata metadata = proxiedMetadata.getKey();
        Map<SchemaTableName, Map<String, ColumnMetadata>> tablesColumnsMetadata = new HashMap<>();
        schemaTableNames.forEach(schemaTableName -> {
            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());
            if (tableHandle != null) {
                Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
                Map<String, ColumnMetadata> columnMetadataMap = columnHandles
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Entry::getKey, entry -> metadata.getColumnMetadata(session, tableHandle, entry.getValue())));
                tablesColumnsMetadata.put(schemaTableName, columnMetadataMap);
            }
        });
        proxiedConnector.commit(proxiedMetadata.getRight());
        return tablesColumnsMetadata;
    }

    private Map<String, ColumnMetadata> getTableColumnsMetadata(Map<SchemaTableName, Map<String, ColumnMetadata>> tablesColumnsMetadata, WarmupRule warmupRule)
    {
        SchemaTableName schemaTableName = getSchemaTableName(warmupRule);
        return tablesColumnsMetadata.get(schemaTableName);
    }

    private Optional<Type> getColumnType(Map<String, ColumnMetadata> columnsMetadata, WarmupRule warmupRule)
    {
        ColumnMetadata columnMetadata = columnsMetadata.get(warmupRule.getWarpColumn().getName());
        if (columnMetadata != null) {
            return Optional.of(columnMetadata.getType());
        }
        String[] parts = warmupRule.getWarpColumn().getName().split("#", -1);
        if (parts.length == 2) {
            columnMetadata = columnsMetadata.get(parts[0]);
            if (columnMetadata != null) {
                if (TypeUtils.isRowType(columnMetadata.getType())) {
                    RowType rowType = (RowType) columnMetadata.getType();
                    return rowType.getFields()
                            .stream()
                            .filter(field -> field.getName().equals(Optional.of(parts[1])))
                            .map(Field::getType)
                            .findAny();
                }
            }
        }
        return Optional.empty();
    }

    private SchemaTableName getSchemaTableName(WarmupRule warmupRule)
    {
        return new SchemaTableName(warmupRule.getSchema(), warmupRule.getTable());
    }

    public long getUniqueRuleId(WarmupRule warmupRule)
    {
        long ret = (2851L * warmupRule.getSchema().hashCode());
        ret += (2917L * warmupRule.getTable().hashCode());
        ret += (2999L * warmupRule.getWarpColumn().hashCode());
        ret += (3061L * warmupRule.getWarmUpType().name().hashCode());
        if (warmupRule.getPredicates() != null) {
            long predicatesHash = warmupRule.getPredicates().stream()
                    .mapToLong(predicateRule -> Hashing.murmur3_128().hashInt(predicateRule.hashCode()).asLong())
                    .sum();
            ret += (3137 * predicatesHash);
        }
        return Hashing.murmur3_128().hashLong(ret).asLong();
    }

    private String getColumnUniqueKey(WarmupRule warmupRule)
    {
        int predicateHash = warmupRule.getPredicates().stream().mapToInt(WarmupPredicateRule::hashCode).sum();
        return String.format(Locale.US, "%s:%s:%s:%s", warmupRule.getSchema(), warmupRule.getTable(), warmupRule.getWarpColumn().getName(), predicateHash);
    }

    private static boolean isNew(WarmupRule warmupRule)
    {
        return warmupRule.getId() == 0;
    }

    private List<WarmupRule> internalSave(Collection<WarmupRule> appliedRules, boolean replace)
    {
        List<WarmupRule> actualAppliedRules = new ArrayList<>();
        ImmutableMap.Builder<Integer, WarmupRule> builder = ImmutableMap.builder();

        appliedRules.forEach(warmupRule -> {
            if (isNew(warmupRule)) {
                warmupRule = WarmupRule.builder(warmupRule).id(idGen.getAndIncrement()).build();
            }
            builder.put(warmupRule.getId(), warmupRule);
            actualAppliedRules.add(warmupRule);
        });
        // add the rest
        if (!replace) {
            Map<Integer, WarmupRule> tmpMap = new HashMap<>(cache);
            appliedRules
                    .forEach(warmupRule -> tmpMap.remove(warmupRule.getId()));
            builder.putAll(tmpMap);
        }

        cache = builder.buildOrThrow();

        return actualAppliedRules;
    }
}
