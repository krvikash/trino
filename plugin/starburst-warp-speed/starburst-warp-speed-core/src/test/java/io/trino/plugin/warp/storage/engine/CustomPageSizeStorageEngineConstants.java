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
package io.trino.plugin.warp.storage.engine;

/**
 * The regular stub with a custom page size
 */
public class CustomPageSizeStorageEngineConstants
        extends StubsStorageEngineConstants
{
    private final int pageSize;

    /**
     * @param pageSize - must be a power of 2
     */
    public CustomPageSizeStorageEngineConstants(int pageSize)
    {
        super();
        this.pageSize = pageSize;
    }

    @Override
    public int getPageSize()
    {
        return pageSize;
    }

    @Override
    public int getPageSizeMask()
    {
        return -1 * pageSize;
    }
}
