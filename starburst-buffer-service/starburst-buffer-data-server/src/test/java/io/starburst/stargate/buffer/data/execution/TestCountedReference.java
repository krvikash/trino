/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import io.starburst.stargate.buffer.data.execution.CountedReference.Ref;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCountedReference
{
    @Test
    public void testOwnerReleaseDestroysResource()
    {
        AtomicInteger destroyCount = new AtomicInteger();
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        ref.release();

        assertThat(destroyCount.get()).isEqualTo(1);
    }

    @Test
    public void testGetReturnsValue()
    {
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> {});

        assertThat(ref.get()).isEqualTo("resource");
        ref.release();
    }

    @Test
    public void testGetAfterDestroyThrows()
    {
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> {});

        ref.release();

        assertThatThrownBy(ref::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("destroyed");
    }

    @Test
    public void testAddedReferenceDefersDestroy()
    {
        AtomicInteger destroyCount = new AtomicInteger();
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        Ref<String> readerRef = ref.addReference();

        ref.release();
        assertThat(destroyCount.get()).isEqualTo(0);

        readerRef.release();
        assertThat(destroyCount.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleRefsDestroyOnlyAfterAllReleased()
    {
        AtomicInteger destroyCount = new AtomicInteger();
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        // Acquire three extra references beyond the initial ref
        Ref<String> ref1 = ref.addReference();
        Ref<String> ref2 = ref.addReference();
        Ref<String> ref3 = ref.addReference();

        // Releasing references one by one does not destroy the resource while others are still held
        ref.release();
        assertThat(destroyCount.get()).isEqualTo(0);

        ref1.release();
        assertThat(destroyCount.get()).isEqualTo(0);

        ref2.release();
        assertThat(destroyCount.get()).isEqualTo(0);

        // Resource is destroyed only when the last reference is released
        ref3.release();
        assertThat(destroyCount.get()).isEqualTo(1);
    }

    @Test
    public void testAddReferenceAfterDestroyThrows()
    {
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> {});

        ref.release();

        assertThatThrownBy(ref::addReference)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testDoubleReleaseThrows()
    {
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> {});

        Ref<String> extraRef = ref.addReference();
        // Release once: count drops from 2 to 1, ref still holds the remaining reference.
        // This ensures the double-release is caught by the per-callback guard, not by count going negative.
        extraRef.release();

        assertThatThrownBy(extraRef::release)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reference already released");
    }

    @Test
    public void testDoubleOwnerReleaseThrows()
    {
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> {});

        ref.release();

        assertThatThrownBy(ref::release)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reference already released");
    }

    @Test
    public void testReaderReleaseWithoutOwnerDoesNotDestroy()
    {
        AtomicInteger destroyCount = new AtomicInteger();
        Ref<String> ref = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        Ref<String> readerRef = ref.addReference();
        readerRef.release();

        assertThat(destroyCount.get()).isEqualTo(0);
    }

    @Test
    public void testDestroyerReceivesValue()
    {
        List<String> destroyed = new ArrayList<>();
        Ref<String> ref = CountedReference.create(
                () -> "my-resource",
                destroyed::add);

        ref.release();

        assertThat(destroyed).containsExactly("my-resource");
    }
}
