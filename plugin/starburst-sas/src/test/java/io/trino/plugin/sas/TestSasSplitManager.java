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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.connector.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

final class TestSasSplitManager
{
    @Test
    void testSplit()
            throws NoSuchFieldException, IllegalAccessException
    {
        Path file = Path.of("src/test/resources").toAbsolutePath();
        SasConfig config = new SasConfig();
        config.setDataDirectory(file.toUri());
        config.setMinPagePerSplit(1000);

        Field splitsField = FixedSplitSource.class.getDeclaredField("splits");
        splitsField.setAccessible(true);

        SasNasClient client = new SasNasClient(Location.of("local:///"), new LocalFileSystemFactory(file));
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        SasTable table = client.getTable("schema1", "colon", identity).orElseThrow();
        SasSplitManager splitManager = new SasSplitManager(config);

        FixedSplitSource splitSource = (FixedSplitSource) splitManager.getSplits(
                null,
                TestingConnectorSession.SESSION,
                new SasTableHandle("schema1", "colon", 5, table.source(), table.pageCount()),
                ImmutableSet.of(),
                null);

        List<ConnectorSplit> splits = (List<ConnectorSplit>) splitsField.get(splitSource);

        assertThat(splits).hasSize(1);

        config.setMinPagePerSplit(1);
        SasSplitManager splitManagerFine = new SasSplitManager(config);
        splitSource = (FixedSplitSource) splitManagerFine.getSplits(
                null,
                TestingConnectorSession.SESSION,
                new SasTableHandle("schema1", "colon", 5, table.source(), table.pageCount()),
                ImmutableSet.of(),
                null);
        splits = (List<ConnectorSplit>) splitsField.get(splitSource);

        assertThat(splits).hasSize(7);
    }

    @Test
    void testSplitCountConfig()
            throws NoSuchFieldException, IllegalAccessException
    {
        Path file = Path.of("src/test/resources").toAbsolutePath();
        SasConfig config = new SasConfig();
        config.setDataDirectory(file.toUri());
        config.setSplitCount(2);
        config.setMinPagePerSplit(1);

        Field splitsField = FixedSplitSource.class.getDeclaredField("splits");
        splitsField.setAccessible(true);

        SasNasClient client = new SasNasClient(Location.of("local:///"), new LocalFileSystemFactory(file));
        ConnectorIdentity identity = ConnectorIdentity.ofUser("test");
        SasTable table = client.getTable("schema1", "colon", identity).orElseThrow();
        SasSplitManager splitManager = new SasSplitManager(config);

        // tableHandle.splits() = -1 → no per-query override, splitCount from config is used
        // colon has 7 pages: pagesPerSplit = max(7/2, 1) = 3 → 3 splits (pages 0-2, 3-5, 6)
        FixedSplitSource splitSource = (FixedSplitSource) splitManager.getSplits(
                null,
                TestingConnectorSession.SESSION,
                new SasTableHandle("schema1", "colon", -1, table.source(), table.pageCount()),
                ImmutableSet.of(),
                null);

        List<ConnectorSplit> splits = (List<ConnectorSplit>) splitsField.get(splitSource);

        assertThat(splits).hasSize(3);
    }
}
