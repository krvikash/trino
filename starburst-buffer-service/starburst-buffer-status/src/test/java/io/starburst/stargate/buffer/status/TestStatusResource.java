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

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStatusResource
{
    private static final String ALL_TRUE_PROVIDER_NAME = "ALL_TRUE";
    private static final MockStatusProvider ALL_TRUE_PROVIDER =
            MockStatusProvider.builder()
                    .withName(ALL_TRUE_PROVIDER_NAME)
                    .build();

    private static final String NOT_STARTED_PROVIDER_NAME = "NOT_STARTED";
    private static final MockStatusProvider NOT_STARTED_PROVIDER =
            MockStatusProvider.builder()
                    .withName(NOT_STARTED_PROVIDER_NAME)
                    .withStarted(false)
                    .build();

    private static final String NOT_READY_PROVIDER_NAME = "NOT_READY";
    private static final MockStatusProvider NOT_READY_PROVIDER =
            MockStatusProvider.builder()
                    .withName(NOT_READY_PROVIDER_NAME)
                    .withReady(false)
                    .build();

    private static final String NOT_ALIVE_PROVIDER_NAME = "NOT_ALIVE";
    private static final MockStatusProvider NOT_ALIVE_PROVIDER =
            MockStatusProvider.builder()
                    .withName(NOT_ALIVE_PROVIDER_NAME)
                    .withLive(false)
                    .build();

    @Test
    public void testAllTrue()
    {
        StatusResource statusResource = new StatusResource(
                new StatusManager(
                        Set.of(ALL_TRUE_PROVIDER)));
        assertThat(statusResource.checkStarted().getStatus()).isEqualTo(200);
        assertThat(statusResource.checkReady().getStatus()).isEqualTo(200);
        assertThat(statusResource.checkAlive().getStatus()).isEqualTo(200);

        ServicesStatus allServicesStatus = statusResource.getServicesStatus();

        Status globalStatus = allServicesStatus.global();
        assertThat(globalStatus.isStarted()).isTrue();
        assertThat(globalStatus.isReady()).isTrue();
        assertThat(globalStatus.isAlive()).isTrue();

        Set<NamedStatus> serviceStatuses = allServicesStatus.services();
        assertThat(serviceStatuses.size()).isEqualTo(1);
        Optional<NamedStatus> provider = findProvider(serviceStatuses, ALL_TRUE_PROVIDER_NAME);
        assertThat(provider).isNotEmpty();
        Status mockProviderStatuses = provider.get().status();
        assertThat(mockProviderStatuses.isStarted()).isTrue();
        assertThat(mockProviderStatuses.isReady()).isTrue();
        assertThat(mockProviderStatuses.isAlive()).isTrue();
    }

    @Test
    public void testNotStarted()
    {
        StatusResource statusResource = new StatusResource(
                new StatusManager(
                        Set.of(ALL_TRUE_PROVIDER, NOT_STARTED_PROVIDER)));
        assertThat(statusResource.checkStarted().getStatus()).isEqualTo(500);
        assertThat(statusResource.checkReady().getStatus()).isEqualTo(200);
        assertThat(statusResource.checkAlive().getStatus()).isEqualTo(200);

        ServicesStatus allServicesStatus = statusResource.getServicesStatus();

        Status globalStatus = allServicesStatus.global();
        assertThat(globalStatus.isStarted()).isFalse();
        assertThat(globalStatus.isReady()).isTrue();
        assertThat(globalStatus.isAlive()).isTrue();

        Set<NamedStatus> serviceStatuses = allServicesStatus.services();
        assertThat(serviceStatuses.size()).isEqualTo(2);
        assertThat(findProvider(serviceStatuses, ALL_TRUE_PROVIDER_NAME)).isNotEmpty();

        Optional<NamedStatus> provider = findProvider(serviceStatuses, NOT_STARTED_PROVIDER_NAME);
        assertThat(provider).isNotEmpty();
        Status mockProviderStatuses = provider.get().status();
        assertThat(mockProviderStatuses.isStarted()).isFalse();
        assertThat(mockProviderStatuses.isReady()).isTrue();
        assertThat(mockProviderStatuses.isAlive()).isTrue();
    }

    @Test
    public void testNotReady()
    {
        StatusResource statusResource = new StatusResource(
                new StatusManager(
                        Set.of(ALL_TRUE_PROVIDER, NOT_READY_PROVIDER)));
        assertThat(statusResource.checkStarted().getStatus()).isEqualTo(200);
        assertThat(statusResource.checkReady().getStatus()).isEqualTo(500);
        assertThat(statusResource.checkAlive().getStatus()).isEqualTo(200);

        ServicesStatus allServicesStatus = statusResource.getServicesStatus();

        Status globalStatus = allServicesStatus.global();
        assertThat(globalStatus.isStarted()).isTrue();
        assertThat(globalStatus.isReady()).isFalse();
        assertThat(globalStatus.isAlive()).isTrue();

        Set<NamedStatus> serviceStatuses = allServicesStatus.services();
        assertThat(serviceStatuses.size()).isEqualTo(2);
        assertThat(findProvider(serviceStatuses, ALL_TRUE_PROVIDER_NAME)).isNotEmpty();

        Optional<NamedStatus> provider = findProvider(serviceStatuses, NOT_READY_PROVIDER_NAME);
        assertThat(provider).isNotEmpty();
        Status mockProviderStatuses = provider.get().status();
        assertThat(mockProviderStatuses.isStarted()).isTrue();
        assertThat(mockProviderStatuses.isReady()).isFalse();
        assertThat(mockProviderStatuses.isAlive()).isTrue();
    }

    @Test
    public void testNotAlive()
    {
        StatusResource statusResource = new StatusResource(
                new StatusManager(
                        Set.of(ALL_TRUE_PROVIDER, NOT_ALIVE_PROVIDER)));
        assertThat(statusResource.checkStarted().getStatus()).isEqualTo(200);
        assertThat(statusResource.checkReady().getStatus()).isEqualTo(200);
        assertThat(statusResource.checkAlive().getStatus()).isEqualTo(500);

        ServicesStatus allServicesStatus = statusResource.getServicesStatus();

        Status globalStatus = allServicesStatus.global();
        assertThat(globalStatus.isStarted()).isTrue();
        assertThat(globalStatus.isReady()).isTrue();
        assertThat(globalStatus.isAlive()).isFalse();

        Set<NamedStatus> serviceStatuses = allServicesStatus.services();
        assertThat(serviceStatuses.size()).isEqualTo(2);
        assertThat(findProvider(serviceStatuses, ALL_TRUE_PROVIDER_NAME)).isNotEmpty();

        Optional<NamedStatus> provider = findProvider(serviceStatuses, NOT_ALIVE_PROVIDER_NAME);
        assertThat(provider).isNotEmpty();
        Status mockProviderStatuses = provider.get().status();
        assertThat(mockProviderStatuses.isStarted()).isTrue();
        assertThat(mockProviderStatuses.isReady()).isTrue();
        assertThat(mockProviderStatuses.isAlive()).isFalse();
    }

    private Optional<NamedStatus> findProvider(Set<NamedStatus> serviceStatuses, String providerName)
    {
        return serviceStatuses.stream().filter(p -> p.name().equals(providerName)).findFirst();
    }
}
