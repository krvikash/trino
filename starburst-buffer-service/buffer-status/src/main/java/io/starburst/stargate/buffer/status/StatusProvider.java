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

/**
 * This interface implementations should follow Kubernetes status probes concepts
 * https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/
 */
public interface StatusProvider
{
    default String getName()
    {
        return this.getClass().getSimpleName();
    }

    /**
     * Indicates that Service is started but may not yet
     * be ready to accept traffic.
     */
    boolean isStarted();

    /**
     * Indicates if service is ready to accept traffic
     */
    boolean isReady();

    /**
     * If false indicates that service is in operational state
     * but encountered issue that is blocking it from making
     * progress and requires restart.
     */
    boolean isAlive();
}
