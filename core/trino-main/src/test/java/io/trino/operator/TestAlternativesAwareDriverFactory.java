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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.execution.ScheduledSplit;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.ValuesOperator.ValuesOperatorFactory;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.split.AlternativeChooser;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.NullOutputOperator.NullOutputOperatorFactory;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingSplit;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.TestingOperatorContext.createDriverContext;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestAlternativesAwareDriverFactory
{
    public static final PlanNodeId CHOOSE_ALTERNATIVE_NODE_ID = new PlanNodeId("chooseAlternative");
    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
    {
        scheduledExecutor = newSingleThreadScheduledExecutor();
    }

    @AfterAll
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testPageSourceProviderCachedPerAlternative()
    {
        // Track created providers to verify caching
        Map<Integer, ConnectorPageSourceProvider> createdProviders = new HashMap<>();
        ConnectorAlternativeChooser connectorAlternativeChooser = (_, _, _) -> {
            int chosenIndex = 0;  // Always choose first alternative
            return new ConnectorAlternativeChooser.Choice(chosenIndex, () -> {
                ConnectorPageSourceProvider provider = new ConnectorPageSourceProvider() {};
                assertThat(createdProviders.putIfAbsent(chosenIndex, provider))
                        .describedAs("Provider factory should only be called once per alternative")
                        .isNull();
                return provider;
            });
        };

        AlternativesAwareDriverFactory factory = new AlternativesAwareDriverFactory(
                new AlternativeChooser(_ -> connectorAlternativeChooser),
                TEST_SESSION,
                alternatives(ImmutableMap.of("alternative0", new MockOperatorFactory())),
                CHOOSE_ALTERNATIVE_NODE_ID,
                Optional.empty(),
                0,
                true,
                false,
                OptionalInt.empty());

        // Create first driver
        Driver driver0 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(0)));
        ConnectorPageSourceProvider provider0 = driver0.getDriverContext().getAlternativePageSourceProvider().orElseThrow();

        // Create second driver for different split but same alternative
        Driver driver1 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(1)));
        ConnectorPageSourceProvider provider1 = driver1.getDriverContext().getAlternativePageSourceProvider().orElseThrow();

        // Verify same instance is reused
        assertThat(provider1).isSameAs(provider0)
                .describedAs("Same page source provider should be reused for same alternative");

        // Verify factory was only called once
        assertThat(createdProviders)
                .describedAs("Provider factory should only be called once even for multiple splits")
                .hasSize(1);
    }

    @Test
    public void testDifferentPageSourceProvidersForDifferentAlternatives()
    {
        // Track created providers per alternative
        Map<Integer, ConnectorPageSourceProvider> createdProviders = new HashMap<>();
        AtomicInteger currentAlternative = new AtomicInteger(0);

        ConnectorAlternativeChooser connectorAlternativeChooser = (_, _, _) -> {
            int chosenIndex = currentAlternative.get();
            return new ConnectorAlternativeChooser.Choice(chosenIndex, () -> {
                ConnectorPageSourceProvider provider = new ConnectorPageSourceProvider() {};
                assertThat(createdProviders.putIfAbsent(chosenIndex, provider))
                        .describedAs("Provider factory should only be called once per alternative")
                        .isNull();
                return provider;
            });
        };

        AlternativesAwareDriverFactory factory = new AlternativesAwareDriverFactory(
                new AlternativeChooser(_ -> connectorAlternativeChooser),
                TEST_SESSION,
                alternatives(ImmutableMap.of(
                        "alternative0", new MockOperatorFactory(),
                        "alternative1", new MockOperatorFactory())),
                CHOOSE_ALTERNATIVE_NODE_ID,
                Optional.empty(),
                0,
                true,
                false,
                OptionalInt.empty());

        // Create driver for alternative 0
        currentAlternative.set(0);
        Driver driver0 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(0)));
        ConnectorPageSourceProvider provider0 = driver0.getDriverContext().getAlternativePageSourceProvider().orElseThrow();

        // Create driver for alternative 1
        currentAlternative.set(1);
        Driver driver1 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(1)));
        ConnectorPageSourceProvider provider1 = driver1.getDriverContext().getAlternativePageSourceProvider().orElseThrow();

        // Verify different providers for different alternatives
        assertThat(provider1).isNotSameAs(provider0)
                .describedAs("Different alternatives should have different providers");

        // Go back to alternative 0
        currentAlternative.set(0);
        Driver driver2 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(2)));
        ConnectorPageSourceProvider provider2 = driver2.getDriverContext().getAlternativePageSourceProvider().orElseThrow();

        // Verify same instance is reused for same alternative
        assertThat(provider2).isSameAs(provider0)
                .describedAs("Same page source provider should be reused for same alternative 0");

        // Create another driver for alternative 1
        currentAlternative.set(1);
        Driver driver3 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(3)));
        ConnectorPageSourceProvider provider3 = driver3.getDriverContext().getAlternativePageSourceProvider().orElseThrow();

        // Verify same instance for same alternative
        assertThat(provider3).isSameAs(provider1)
                .describedAs("Same page source provider should be reused for same alternative 1");

        // Verify factories were only once per alternative
        assertThat(createdProviders)
                .describedAs("Provider factory should only be called once per each alternative")
                .hasSize(2);
    }

    @Test
    public void testCorrectAlternativeDriversCreated()
    {
        AtomicInteger currentAlternative = new AtomicInteger(0);
        ConnectorAlternativeChooser connectorAlternativeChooser = (_, _, _) ->
                new ConnectorAlternativeChooser.Choice(currentAlternative.get(), () -> new ConnectorPageSourceProvider() {});

        MockOperatorFactory alternativeOperatorFactory0 = new MockOperatorFactory();
        MockOperatorFactory alternativeOperatorFactory1 = new MockOperatorFactory();
        AlternativesAwareDriverFactory factory = new AlternativesAwareDriverFactory(
                new AlternativeChooser(_ -> connectorAlternativeChooser),
                TEST_SESSION,
                alternatives(ImmutableMap.of(
                        "alternative0", alternativeOperatorFactory0,
                        "alternative1", alternativeOperatorFactory1)),
                CHOOSE_ALTERNATIVE_NODE_ID,
                Optional.empty(),
                0,
                true,
                false,
                OptionalInt.empty());

        Driver driver0 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(0)));
        assertThat(alternativeOperatorFactory0.createdOperators).isEqualTo(1);
        assertThat(driver0.getDriverContext().getAlternativePageSourceProvider()).isPresent();
        assertThat(driver0.getDriverContext().getAlternativeId()).hasValue(0);

        currentAlternative.set(1);
        Driver driver1 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(1)));
        assertThat(alternativeOperatorFactory0.createdOperators).isEqualTo(1);
        assertThat(driver1.getDriverContext().getAlternativePageSourceProvider()).isPresent();
        assertThat(driver1.getDriverContext().getAlternativeId()).hasValue(1);

        currentAlternative.set(0);
        Driver driver2 = factory.createDriver(createDriverContext(scheduledExecutor), Optional.of(split(2)));
        assertThat(alternativeOperatorFactory0.createdOperators).isEqualTo(2);
        assertThat(driver2.getDriverContext().getAlternativePageSourceProvider()).isPresent();
        assertThat(driver2.getDriverContext().getAlternativeId()).hasValue(0);
    }

    private static ScheduledSplit split(int sequenceId)
    {
        return new ScheduledSplit(sequenceId, CHOOSE_ALTERNATIVE_NODE_ID, new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
    }

    private static Map<TableHandle, DriverFactory> alternatives(Map<String, MockOperatorFactory> alternatives)
    {
        return alternatives.entrySet().stream().collect(toImmutableMap(
                entry -> new TableHandle(TEST_CATALOG_HANDLE, new TestingTableHandle(new SchemaTableName("test", entry.getKey())), TestingTransactionHandle.create()),
                entry -> new DriverFactory(
                        0,
                        true,
                        false,
                        ImmutableList.of(entry.getValue(), new NullOutputOperatorFactory(1, new PlanNodeId("out"))),
                        OptionalInt.empty())));
    }

    private static class MockOperatorFactory
            implements OperatorFactory
    {
        private final ValuesOperatorFactory delegate;
        private int createdOperators;

        MockOperatorFactory()
        {
            this(new ValuesOperatorFactory(0, new PlanNodeId("0"), ImmutableList.of()));
        }

        MockOperatorFactory(ValuesOperatorFactory delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            createdOperators++;
            return delegate.createOperator(driverContext);
        }

        @Override
        public void noMoreOperators() {}

        @Override
        public OperatorFactory duplicate()
        {
            return new MockOperatorFactory(delegate);
        }
    }
}
