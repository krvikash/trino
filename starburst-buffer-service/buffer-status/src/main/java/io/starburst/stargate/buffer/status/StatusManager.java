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

import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;

public class StatusManager
{
    private final Set<StatusProvider> statusProviders;

    @Inject
    public StatusManager(Set<StatusProvider> statusProviders)
    {
        this.statusProviders = ImmutableSet.copyOf(requireNonNull(statusProviders, "statusProviders is null"));
    }

    public boolean checkStarted()
    {
        return checkState(StatusProvider::isStarted);
    }

    public boolean checkReady()
    {
        return checkState(StatusProvider::isReady);
    }

    public boolean checkAlive()
    {
        return checkState(StatusProvider::isAlive);
    }

    public ServicesStatus getServicesStatus()
    {
        return new ServicesStatus(
                new Status(checkStarted(), checkReady(), checkAlive()),
                statusProviders.stream()
                        .map(statusProvider -> new NamedStatus(
                                statusProvider.getName(),
                                new Status(
                                        statusProvider.isStarted(),
                                        statusProvider.isReady(),
                                        statusProvider.isAlive())))
                        .collect(toSet()));
    }

    private boolean checkState(Function<StatusProvider, Boolean> statusExtractor)
    {
        return statusProviders.stream()
                .filter(not(statusExtractor::apply))
                .findFirst()
                .isEmpty();
    }
}
