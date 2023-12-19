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

import java.util.Set;

import static java.util.Objects.requireNonNull;

public record ServicesStatus(Status global, Set<NamedStatus> services)
{
    public ServicesStatus
    {
        requireNonNull(global, "global is null");
        requireNonNull(services, "services is null");
    }
}
