/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.discovery.client.failures;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record FailureInfo(
        Optional<String> exchangeId,
        String observingClient,
        String failureDetails)
{
    public FailureInfo {
        requireNonNull(exchangeId, "exchangeId is null");
        requireNonNull(observingClient, "observingClient is null");
        requireNonNull(failureDetails, "exceptionMessage is null");
    }
}
