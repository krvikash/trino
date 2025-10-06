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
package io.trino.testng.services;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSuccessRate
{
    @Test
    void testNoFailures()
    {
        Fixture fixture = new Fixture(1);
        assertEventually(
                new Duration(3, SECONDS),
                new Duration(1, SECONDS),
                3,
                0.75f,
                () -> assertThat(fixture.doWork()).isEqualTo(1));
    }

    @Test
    void testThresholdMet()
    {
        Fixture fixture = new Fixture(0, 1, 1, 1, 1);
        assertEventually(
                new Duration(3, SECONDS),
                new Duration(50, MILLISECONDS),
                6,
                0.75f,
                () -> assertThat(fixture.doWork()).isEqualTo(1));
    }

    @Test
    void testThresholdNotMetTimeout()
    {
        Fixture fixture = new Fixture(0, 1, 0, 1, 1);
        assertThatThrownBy(
                () -> assertEventually(
                        new Duration(3, SECONDS),
                        new Duration(1, SECONDS),
                        5,
                        0.75f,
                        () -> assertThat(fixture.doWork()).isEqualTo(1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("is below the minimum required 75.0%")
                .cause()
                .hasMessageContaining("\nexpected: 1\n but was: 0");
    }

    @Test
    void testThresholdNotMetMaxRetries()
    {
        Fixture fixture = new Fixture(0, 1, 1, 1, 1);
        assertThatThrownBy(
                () -> assertEventually(
                        new Duration(5, SECONDS),
                        new Duration(50, MILLISECONDS),
                        2,
                        0.75f,
                        () -> assertThat(fixture.doWork()).isEqualTo(1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("is below the minimum required 75.0%")
                .cause()
                .hasMessageContaining("\nexpected: 1\n but was: 0");
    }

    @Test
    void testOnlyFailures()
    {
        Fixture fixture = new Fixture(0, 0, 0, 0, 0, 0, 0);
        assertThatThrownBy(
                () -> assertEventually(
                        new Duration(3, SECONDS),
                        new Duration(20, MILLISECONDS),
                        5,
                        0.75f,
                        () -> assertThat(fixture.doWork()).isEqualTo(1)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("\nexpected: 1\n but was: 0");
    }

    @Test
    void testNoThreshold()
    {
        Fixture fixture = new Fixture(0, 0, 0, 0, 1);
        assertEventually(() -> assertThat(fixture.doWork()).isEqualTo(1));
    }

    static class Fixture
    {
        private int invocations;
        private final List<Integer> returnValues;

        Fixture(Integer... returnValues)
        {
            this.returnValues = ImmutableList.copyOf(returnValues);
        }

        int doWork()
        {
            return returnValues.get(invocations++);
        }
    }
}
