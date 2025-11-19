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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.vbyte.VByteNative;
import io.trino.FeaturesConfig;
import io.trino.block.DictionaryAdaptiveBlockEncoding;
import io.trino.block.IntArrayAdaptiveBlockEncoding;
import io.trino.block.LongArrayAdaptiveBlockEncoding;
import io.trino.block.VariableWidthAdaptiveBlockEncoding;
import io.trino.simd.BlockEncodingSimdSupport;
import io.trino.simd.BlockEncodingSimdSupport.SimdSupport;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class BlockEncodingManager
{
    private static final Logger log = Logger.get(VByteNative.class);

    // for deserialization
    private final Map<String, BlockEncoding> blockEncodingsByName = new ConcurrentHashMap<>();

    // for serialization
    private final Map<Class<? extends Block>, BlockEncoding> blockEncodingNamesByClass = new ConcurrentHashMap<>();

    @Inject
    public BlockEncodingManager(FeaturesConfig config, BlockEncodingSimdSupport blockEncodingSimdSupport)
    {
        // add the built-in BlockEncodings
        SimdSupport simdSupport = blockEncodingSimdSupport.getSimdSupport();
        addBlockEncoding(new ByteArrayBlockEncoding(simdSupport.expandAndCompressByte()));
        addBlockEncoding(new ShortArrayBlockEncoding(simdSupport.expandAndCompressShort()));
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
            addBlockEncoding(new VariableWidthAdaptiveBlockEncoding(vByteEncodingEnabled));
        }
        else {
            addBlockEncoding(new IntArrayBlockEncoding(simdSupport.expandAndCompressInt()));
            addBlockEncoding(new LongArrayBlockEncoding(simdSupport.expandAndCompressLong()));
            addBlockEncoding(new DictionaryBlockEncoding());
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
}
