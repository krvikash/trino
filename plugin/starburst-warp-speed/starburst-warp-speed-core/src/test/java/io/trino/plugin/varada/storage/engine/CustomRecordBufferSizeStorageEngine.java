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
package io.trino.plugin.varada.storage.engine;

import java.util.Arrays;

/**
 * The regular stub except that all record buffers will be of the same fixed size
 */
public class CustomRecordBufferSizeStorageEngine
        extends StubsStorageEngine
{
    private final int recordBufferSize;

    /**
     * @param recordBufferSize - must be a power of 2
     */
    public CustomRecordBufferSizeStorageEngine(int recordBufferSize)
    {
        super();
        this.recordBufferSize = recordBufferSize;
    }

    @Override
    public void initRecordBufferSizes(int[] fixedRecordBufferSizes, int[] varlenRecordBufferSizes)
    {
        super.initRecordBufferSizes(fixedRecordBufferSizes, varlenRecordBufferSizes);
        Arrays.fill(fixedRecordBufferSizes, recordBufferSize);
    }
}
