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
package io.trino.spi;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPageCapacityEstimator
{
    @Test
    void rejectsInvalidConstructorArgs()
    {
        assertThatThrownBy(() -> new PageCapacityEstimator(0, 100, 1024))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PageCapacityEstimator(-1, 100, 1024))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PageCapacityEstimator(10, 5, 1024))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PageCapacityEstimator(10, 100, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void initialState()
    {
        PageCapacityEstimator estimator = new PageCapacityEstimator(8, 1024, 1_000_000);
        assertThat(estimator.currentCapacity()).isEqualTo(8);
        assertThat(estimator.isFull(0)).isFalse();
        assertThat(estimator.isFull(7)).isFalse();
        assertThat(estimator.isFull(8)).isTrue();
        assertThat(estimator.isFull(100)).isTrue();
    }

    @Test
    void zeroPositionsIsNoOp()
    {
        PageCapacityEstimator estimator = new PageCapacityEstimator(8, 1024, 1_000_000);
        estimator.recordPage(0, 0);
        assertThat(estimator.currentCapacity()).isEqualTo(8);
        estimator.recordPage(1024, 0);
        assertThat(estimator.currentCapacity()).isEqualTo(8);
    }

    @Test
    void emptyMaxAverageKeepsCapacityFromInitial()
    {
        // recordPage with zero-byte page keeps maxAverageBytesPerPosition empty; capacity stays.
        PageCapacityEstimator estimator = new PageCapacityEstimator(8, 1024, 1_000_000);
        estimator.recordPage(0, 8);
        // maxAverage is empty -> capacity unchanged
        assertThat(estimator.currentCapacity()).isEqualTo(8);
    }

    @Test
    void doublesUpToByteBudget()
    {
        // bytes/pos = 10, maxPage = 1_000_000 -> byte budget = 100_000 positions.
        // maxEntries = 1024 (binding upper bound).
        PageCapacityEstimator estimator = new PageCapacityEstimator(8, 1024, 1_000_000);

        estimator.recordPage(80, 8); // 10 bytes/pos
        // next = min(1024, min(2*8, 100_000)) = 16
        assertThat(estimator.currentCapacity()).isEqualTo(16);

        estimator.recordPage(160, 16);
        assertThat(estimator.currentCapacity()).isEqualTo(32);

        // Eventually caps at maxEntries.
        int last = 32;
        for (int i = 0; i < 20; i++) {
            estimator.recordPage(last * 10L, last);
            last = estimator.currentCapacity();
        }
        assertThat(estimator.currentCapacity()).isEqualTo(1024);
    }

    @Test
    void shrinksWhenBytesPerPositionExceedsBudget()
    {
        // maxPage = 1000, bytes/pos = 100 -> byte budget = 10 positions.
        // initial capacity = 64 but budget says 10.
        PageCapacityEstimator estimator = new PageCapacityEstimator(64, 1024, 1000);
        estimator.recordPage(64 * 100L, 64); // 100 bytes/pos
        // next = min(1024, min(2*64, 1000/100=10)) = 10
        assertThat(estimator.currentCapacity()).isEqualTo(10);
    }

    @Test
    void capacityNeverDropsBelowOne()
    {
        // avg bytes/pos = 2000 -> byte budget = 0.5 positions, which truncates to 0.
        PageCapacityEstimator estimator = new PageCapacityEstimator(8, 1024, 1000);
        estimator.recordPage(8 * 2000L, 8);
        assertThat(estimator.currentCapacity()).isEqualTo(1);
    }

    @Test
    void maxAverageIsMonotonic()
    {
        PageCapacityEstimator estimator = new PageCapacityEstimator(8, 1024, 1000);
        // First page: 10 bytes/pos -> budget = 100 positions.
        estimator.recordPage(80, 8);
        assertThat(estimator.currentCapacity()).isEqualTo(16);

        // Second page: 200 bytes/pos -> budget = 5 positions; maxAverage updated to 200.
        estimator.recordPage(16 * 200L, 16);
        assertThat(estimator.currentCapacity()).isEqualTo(5);

        // Third page: back to 10 bytes/pos. Should NOT relax because maxAverage stays at 200.
        estimator.recordPage(5 * 10L, 5);
        assertThat(estimator.currentCapacity()).isEqualTo(5);
    }
}
