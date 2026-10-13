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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.spi.connector.IncrementalColumnWatermark;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.iceberg.IncrementalColumn.findWithExactUpperBound;
import static io.trino.spi.connector.IncrementalColumnWatermark.Empty.emptyIncrementalColumnWatermark;
import static io.trino.spi.connector.IncrementalColumnWatermark.Unknown.unknownIncrementalColumnWatermark;
import static io.trino.spi.connector.IncrementalColumnWatermark.Value.resolvedIncrementalColumnWatermark;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;

public final class IncrementalColumnWatermarkReader
{
    public static final String TRINO_INCREMENTAL_COLUMN_WATERMARK = "trino-incremental-column-watermark";

    private static final Logger log = Logger.get(IncrementalColumnWatermarkReader.class);
    static final JsonCodec<StoredWatermark> STORED_WATERMARK_CODEC = jsonCodec(StoredWatermark.class);

    private final TypeManager typeManager;

    @Inject
    public IncrementalColumnWatermarkReader(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public StorageTableWatermark readWatermark(String columnName, Table storageTable, OptionalLong storageSnapshotId)
    {
        Optional<IncrementalColumn> incrementalColumn = findWithExactUpperBound(storageTable.schema(), columnName, typeManager);
        if (storageSnapshotId.isEmpty()) {
            return new StorageTableWatermark(incrementalColumn, emptyIncrementalColumnWatermark());
        }
        Snapshot snapshot = storageTable.snapshot(storageSnapshotId.orElseThrow());
        if (snapshot == null || incrementalColumn.isEmpty()) {
            return new StorageTableWatermark(incrementalColumn, unknownIncrementalColumnWatermark());
        }
        return new StorageTableWatermark(incrementalColumn, readIncrementalColumnWatermark(snapshot, incrementalColumn.orElseThrow(), typeManager));
    }

    static IncrementalColumnWatermark readIncrementalColumnWatermark(Snapshot snapshot, IncrementalColumn incrementalColumn, TypeManager typeManager)
    {
        return readIncrementalColumnWatermark(requireNonNullElse(snapshot.summary(), ImmutableMap.of()), incrementalColumn, typeManager);
    }

    static IncrementalColumnWatermark readIncrementalColumnWatermark(Map<String, String> snapshotSummary, IncrementalColumn incrementalColumn, TypeManager typeManager)
    {
        String storedWatermark = snapshotSummary.get(TRINO_INCREMENTAL_COLUMN_WATERMARK);
        if (storedWatermark == null) {
            if ("0".equals(snapshotSummary.get(TOTAL_RECORDS_PROP))) {
                return emptyIncrementalColumnWatermark();
            }
            log.debug("Storage table snapshot carries no watermark for incremental column %s, falling back to the correlated subquery predicate", incrementalColumn.name());
            return unknownIncrementalColumnWatermark();
        }
        return decodeWatermark(storedWatermark, incrementalColumn, typeManager)
                .map(value -> resolvedIncrementalColumnWatermark(incrementalColumn.trinoType(), value))
                .orElseGet(() -> {
                    log.debug("Storage table snapshot carries a watermark for a different column or type than incremental column %s, ignoring it", incrementalColumn.name());
                    return unknownIncrementalColumnWatermark();
                });
    }

    static Optional<Object> decodeWatermark(String json, IncrementalColumn incrementalColumn, TypeManager typeManager)
    {
        StoredWatermark storedWatermark;
        try {
            storedWatermark = parseStoredWatermark(json);
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
        if (storedWatermark.fieldId() != incrementalColumn.field().fieldId()) {
            return Optional.empty();
        }
        try {
            if (!typeManager.getType(storedWatermark.type()).equals(incrementalColumn.trinoType())) {
                return Optional.empty();
            }
            return Optional.of(decodeValue(incrementalColumn.trinoType(), storedWatermark.value()));
        }
        catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static Object decodeValue(Type type, String value)
    {
        if (type instanceof DecimalType decimalType && !decimalType.isShort()) {
            return Int128.valueOf(new BigInteger(value));
        }
        return Long.parseLong(value);
    }

    static StoredWatermark parseStoredWatermark(String json)
    {
        return STORED_WATERMARK_CODEC.fromJson(json);
    }

    /**
     * {@code incrementalColumn} is empty when the storage column cannot hold an exact maximum, in which case no
     * watermark can be read or written for it.
     */
    public record StorageTableWatermark(Optional<IncrementalColumn> incrementalColumn, IncrementalColumnWatermark watermark)
    {
        public StorageTableWatermark
        {
            requireNonNull(incrementalColumn, "incrementalColumn is null");
            requireNonNull(watermark, "watermark is null");
        }
    }

    /**
     * The field id and type travel with the value because neither the column behind a name nor its
     * type is fixed for the lifetime of a storage table: dropping and re-adding a column keeps its
     * name but not its field id, and a schema change writes no snapshot that would drop the property.
     */
    public record StoredWatermark(
            @JsonProperty("fieldId") int fieldId,
            @JsonProperty("type") TypeId type,
            @JsonProperty("value") String value)
    {
        public StoredWatermark
        {
            requireNonNull(type, "type is null");
            requireNonNull(value, "value is null");
        }
    }
}
