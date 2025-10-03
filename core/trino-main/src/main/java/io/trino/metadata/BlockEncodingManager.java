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
package io.trino.metadata;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.vbyte.VByteNative;
import io.trino.FeaturesConfig;
import io.trino.block.DictionaryAdaptiveBlockEncoding;
import io.trino.block.IntArrayAdaptiveBlockEncoding;
import io.trino.block.LongArrayAdaptiveBlockEncoding;
import io.trino.block.VariableWidthVByteBlockEncoding;
import io.trino.spi.block.ArrayBlockEncoding;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.ByteArrayBlockEncoding;
import io.trino.spi.block.DictionaryBlockEncoding;
import io.trino.spi.block.Fixed12BlockEncoding;
import io.trino.spi.block.Int128ArrayBlockEncoding;
import io.trino.spi.block.IntArrayBlockEncoding;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.block.MapBlockEncoding;
import io.trino.spi.block.RowBlockEncoding;
import io.trino.spi.block.RunLengthBlockEncoding;
import io.trino.spi.block.ShortArrayBlockEncoding;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class BlockEncodingManager
{
    private static final Logger log = Logger.get(VByteNative.class);

    // for deserialization
    private final Map<String, BlockEncoding> blockEncodingsByName = new ConcurrentHashMap<>();

    // for serialization
    // default block encoding per block
    private final Map<Class<? extends Block>, BlockEncoding> blockEncodingNamesByClass = new ConcurrentHashMap<>();
    // overrides per type
    private final ListMultimap<Class<? extends Block>, Function<Type, Optional<BlockEncoding>>> blockEncodingsPerTypeOverrides = ArrayListMultimap.create();

    @Inject
    public BlockEncodingManager(FeaturesConfig config)
    {
        // add the built-in BlockEncodings
        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new Fixed12BlockEncoding());
        addBlockEncoding(new Int128ArrayBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new MapBlockEncoding());
        addBlockEncoding(new RowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());

        boolean vByteEncodingEnabled = false;
        if (config.isExchangeVbyteBlockEncodingEnabled()) {
            if (VByteNative.getLinkageError().isPresent()) {
                log.warn(VByteNative.getLinkageError().orElseThrow(), "VByte block encoding disabled because of linkage error");
            }
            else {
                vByteEncodingEnabled = true;
            }
        }
        if (config.isExchangeAdaptiveBlockEncodingEnabled()) {
            addBlockEncoding(new IntArrayAdaptiveBlockEncoding(vByteEncodingEnabled));
            addBlockEncoding(new LongArrayAdaptiveBlockEncoding(vByteEncodingEnabled));
            addBlockEncoding(new DictionaryAdaptiveBlockEncoding(vByteEncodingEnabled));
        }
        else {
            addBlockEncoding(new IntArrayBlockEncoding());
            addBlockEncoding(new LongArrayBlockEncoding());
            addBlockEncoding(new DictionaryBlockEncoding());
        }
        if (vByteEncodingEnabled) {
            addBlockEncoding(new VariableWidthVByteBlockEncoding());
        }
        else {
            addBlockEncoding(new VariableWidthBlockEncoding());
        }
    }

    public BlockEncoding getBlockEncodingByName(String encodingName)
    {
        BlockEncoding blockEncoding = blockEncodingsByName.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding: %s", encodingName);
        return blockEncoding;
    }

    public BlockEncoding getBlockEncodingByBlockClass(Class<? extends Block> clazz)
    {
        return getBlockEncodingByBlockClassAndType(clazz, Optional.empty());
    }

    public BlockEncoding getBlockEncodingByBlockClassAndType(Class<? extends Block> clazz, Optional<Type> type)
    {
        if (type.isPresent()) {
            for (Function<Type, Optional<BlockEncoding>> entry : blockEncodingsPerTypeOverrides.get(clazz)) {
                Optional<BlockEncoding> blockEncoding = entry.apply(type.orElseThrow());
                if (blockEncoding.isPresent()) {
                    // type specific block encoding found.
                    return blockEncoding.get();
                }
            }
        }

        // default block encoding
        BlockEncoding blockEncoding = blockEncodingNamesByClass.get(clazz);
        checkArgument(blockEncoding != null, "Unknown block encoding for block: %s", clazz.getName());
        return blockEncoding;
    }

    public void addBlockEncoding(BlockEncoding blockEncoding)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncoding existingEntryByClass = blockEncodingNamesByClass.putIfAbsent(blockEncoding.getBlockClass(), blockEncoding);
        checkArgument(existingEntryByClass == null, "Encoding already registered: %s", blockEncoding.getName());
        BlockEncoding existingEntryByName = blockEncodingsByName.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntryByName == null, "Encoding already registered: %s", blockEncoding.getName());
    }

    public void addTypeSpecificBlockEncodingOverride(BlockEncoding blockEncoding, Predicate<Type> typePredicate)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        requireNonNull(typePredicate, "type is null");

        // ensure we have entry in blockEncodingsByName
        if (!blockEncodingsByName.containsKey(blockEncoding.getName())) {
            blockEncodingsByName.put(blockEncoding.getName(), blockEncoding);
        }

        blockEncodingsPerTypeOverrides.put(blockEncoding.getBlockClass(),
                type -> {
                    if (typePredicate.test(type)) {
                        return Optional.of(blockEncoding);
                    }
                    return Optional.empty();
                });
    }
}
