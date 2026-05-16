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
package io.trino.plugin.base.gpu;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestClosingRef
{
    @Test
    public void testBorrowAndTake()
    {
        TestCloseable value = new TestCloseable();
        ClosingRef<TestCloseable> ref = ClosingRef.own(value);
        assertThat(ref.borrow()).isSameAs(value);
        assertThat(ref.take()).isSameAs(value);
        assertThatThrownBy(ref::borrow)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No value");
        ref.close();
        assertThat(value.isClosed()).isFalse();
    }

    @Test
    public void testCloseClosesValue()
    {
        TestCloseable value = new TestCloseable();
        ClosingRef<TestCloseable> ref = ClosingRef.own(value);
        ref.close();
        assertThat(value.isClosed()).isTrue();
    }

    @Test
    public void testCloseIsIdempotent()
    {
        TestCloseable value = new TestCloseable();
        ClosingRef<TestCloseable> ref = ClosingRef.own(value);
        ref.close();
        ref.close();
        assertThat(value.closeCount()).isEqualTo(1);
    }

    @Test
    public void testBorrowAfterCloseFails()
    {
        ClosingRef<TestCloseable> ref = ClosingRef.own(new TestCloseable());
        ref.close();
        assertThatThrownBy(ref::borrow)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testTakeAfterCloseFails()
    {
        ClosingRef<TestCloseable> ref = ClosingRef.own(new TestCloseable());
        ref.close();
        assertThatThrownBy(ref::take)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSetAfterCloseClosesIncomingValue()
    {
        ClosingRef<TestCloseable> ref = ClosingRef.empty();
        ref.close();
        TestCloseable newValue = new TestCloseable();
        assertThatThrownBy(() -> ref.set(newValue))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Already closed");
        assertThat(newValue.isClosed()).isTrue();
    }

    @Test
    public void testSetWhenValueAlreadySetClosesIncomingValue()
    {
        ClosingRef<TestCloseable> ref = ClosingRef.own(new TestCloseable());
        TestCloseable newValue = new TestCloseable();
        assertThatThrownBy(() -> ref.set(newValue))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Value already set");
        assertThat(newValue.isClosed()).isTrue();
    }

    @Test
    public void testSetOnEmpty()
    {
        ClosingRef<TestCloseable> ref = ClosingRef.empty();
        TestCloseable value = new TestCloseable();
        ref.set(value);
        assertThat(ref.borrow()).isSameAs(value);
        ref.close();
        assertThat(value.isClosed()).isTrue();
    }

    private static class TestCloseable
            implements AutoCloseable
    {
        private int closeCount;

        @Override
        public void close()
        {
            closeCount++;
        }

        public boolean isClosed()
        {
            return closeCount > 0;
        }

        public int closeCount()
        {
            return closeCount;
        }
    }
}
