/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.status;

import static java.util.Objects.requireNonNull;

public record NamedStatus(String name, Status status)
{
    public NamedStatus
    {
        requireNonNull(name, "name is null");
        requireNonNull(status, "status is null");
    }
}
