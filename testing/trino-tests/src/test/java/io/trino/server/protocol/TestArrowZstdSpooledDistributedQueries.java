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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArrowZstdSpooledDistributedQueries
        extends AbstractSpooledQueryDataDistributedQueries
{
    @Override
    protected Map<String, String> spoolingConfig()
    {
        return ImmutableMap.of("protocol.spooling.encoding.arrow+zstd.enabled", "true");
    }

    @Override
    protected String encoding()
    {
        return "arrow-preview+zstd";
    }

    @Test
    @Override // TODO https://starburstdata.atlassian.net/browse/ENG-7894 Support NUMBER in Trino protocol spooling to Arrow
    public void testNumber()
    {
        assertThatThrownBy(super::testNumber)
                .hasMessage("Output columns [OutputColumn[sourcePageChannel=0, columnName=_col0, type=number]] are not supported for spooling encoding 'arrow-preview+zstd'");
    }
}
