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

import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Objects.requireNonNull;

public record AddDataPagesResult(
        ListenableFuture<Void> addDataPagesFuture,
        boolean shouldRetainMemory)
{
    public AddDataPagesResult
    {
        requireNonNull(addDataPagesFuture, "addDataPagesFuture is null");
    }
}
