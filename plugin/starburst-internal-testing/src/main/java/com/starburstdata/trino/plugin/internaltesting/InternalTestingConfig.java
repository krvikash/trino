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
package com.starburstdata.trino.plugin.internaltesting;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class InternalTestingConfig
{
    private DataSize oomAllocationSize = DataSize.of(100L, GIGABYTE);

    @Config("internal-testing.oom-allocation-size")
    public void setOOMAllocationSize(DataSize oomAllocationSize)
    {
        this.oomAllocationSize = oomAllocationSize;
    }

    public DataSize getOOMAllocationSize()
    {
        return oomAllocationSize;
    }
}
