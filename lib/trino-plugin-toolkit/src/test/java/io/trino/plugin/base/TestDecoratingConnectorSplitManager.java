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
package io.trino.plugin.base;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorSplitManager;
import org.junit.jupiter.api.Test;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;

public class TestDecoratingConnectorSplitManager
{
    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorSplitManager.class, DecoratingConnectorSplitManager.class);
    }

    @Test
    public void testProperForwardingMethodsAreCalled()
    {
        assertProperForwardingMethodsAreCalled(
                ConnectorSplitManager.class,
                splitManager -> new DecoratingConnectorSplitManager(
                        splitManager,
                        ImmutableSet.of(decoratedSplitManager -> decoratedSplitManager)));
    }

    @Test
    public void testSplitManagerAreDecorated()
    {
        assertProperForwardingMethodsAreCalled(
                ConnectorSplitManager.class,
                splitManager -> new DecoratingConnectorSplitManager(
                        (_, _, _, _, _) -> null,
                        ImmutableSet.of(_ -> splitManager)));
    }
}
