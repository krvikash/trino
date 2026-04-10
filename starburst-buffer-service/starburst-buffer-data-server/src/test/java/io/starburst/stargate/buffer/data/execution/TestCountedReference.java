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

import io.starburst.stargate.buffer.data.execution.CountedReference.Handle;
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
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        handle.release();

        assertThat(destroyCount.get()).isEqualTo(1);
    }

    @Test
    public void testGetReturnsValue()
    {
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> {});

        assertThat(handle.get()).isEqualTo("resource");
        handle.release();
    }

    @Test
    public void testGetAfterDestroyThrows()
    {
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> {});

        handle.release();

        assertThatThrownBy(handle::get)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("destroyed");
    }

    @Test
    public void testAddedReferenceDefersDestroy()
    {
        AtomicInteger destroyCount = new AtomicInteger();
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        Runnable readerRelease = handle.addReference();

        handle.release();
        assertThat(destroyCount.get()).isEqualTo(0);

        readerRelease.run();
        assertThat(destroyCount.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleRefsDestroyOnlyAfterAllReleased()
    {
        AtomicInteger destroyCount = new AtomicInteger();
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        // Acquire three extra references beyond the initial handle
        Runnable release1 = handle.addReference();
        Runnable release2 = handle.addReference();
        Runnable release3 = handle.addReference();

        // Releasing references one by one does not destroy the resource while others are still held
        handle.release();
        assertThat(destroyCount.get()).isEqualTo(0);

        release1.run();
        assertThat(destroyCount.get()).isEqualTo(0);

        release2.run();
        assertThat(destroyCount.get()).isEqualTo(0);

        // Resource is destroyed only when the last reference is released
        release3.run();
        assertThat(destroyCount.get()).isEqualTo(1);
    }

    @Test
    public void testAddReferenceAfterDestroyThrows()
    {
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> {});

        handle.release();

        assertThatThrownBy(handle::addReference)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testDoubleReleaseThrows()
    {
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> {});

        Runnable releaseCallback = handle.addReference();
        // Release once: count drops from 2 to 1, handle still holds the remaining reference.
        // This ensures the double-release is caught by the per-callback guard, not by count going negative.
        releaseCallback.run();

        assertThatThrownBy(releaseCallback::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reference already released");
    }

    @Test
    public void testDoubleOwnerReleaseThrows()
    {
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> {});

        handle.release();

        assertThatThrownBy(handle::release)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("reference already released");
    }

    @Test
    public void testReaderReleaseWithoutOwnerDoesNotDestroy()
    {
        AtomicInteger destroyCount = new AtomicInteger();
        Handle<String> handle = CountedReference.create(() -> "resource", _ -> destroyCount.incrementAndGet());

        Runnable readerRelease = handle.addReference();
        readerRelease.run();

        assertThat(destroyCount.get()).isEqualTo(0);
    }

    @Test
    public void testDestroyerReceivesValue()
    {
        List<String> destroyed = new ArrayList<>();
        Handle<String> handle = CountedReference.create(
                () -> "my-resource",
                destroyed::add);

        handle.release();

        assertThat(destroyed).containsExactly("my-resource");
    }
}
