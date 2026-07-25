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
package io.trino.plugin.warp.storage.write;

import io.trino.plugin.warp.storage.juffers.RecordBufferParams;
import io.trino.plugin.warp.storage.memory.ThreadArena;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.Optional;

/* all memory resources are allocated once and are held in this record for all elements to use */
public record StorageWriterSplitConfig(
        String nodeIdentifier,
        String rowGroupFilePath,
        SegmentAllocator warmMemoryAllocator, // refernce to the warm memory slicer to be kept until it should be released
        MemorySegment buff, // segment buffer to allocate different juffers
        MemorySegment writeBuff, // native buffer to use for compression and write to disk
        SegmentAllocator contextAllocator, // allocator for slicing the context buffer
        Optional<WarmUpState> warmUpStateOpt, // warm state used during a single element warming, if not present will be allocated per WE
        RecordBufferParams recordBufferParams, // record buffer parameters used for every commit call
        Optional<CompressionState> compressionStateOpt, // compression state used during a single element warming, if not present will be allocated per WE
        ThreadArena arena) {}
