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

import java.util.UUID;

public class MockStatusProvider
        implements StatusProvider
{
    public static Builder builder()
    {
        return new Builder();
    }

    private final String name;
    private final boolean isStarted;
    private final boolean isReady;
    private final boolean isLive;

    public MockStatusProvider(String name, boolean isStarted, boolean isReady, boolean isLive)
    {
        this.name = name;
        this.isStarted = isStarted;
        this.isReady = isReady;
        this.isLive = isLive;
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public boolean isStarted()
    {
        return isStarted;
    }

    @Override
    public boolean isReady()
    {
        return isReady;
    }

    @Override
    public boolean isAlive()
    {
        return isLive;
    }

    public static class Builder
    {
        private String name = "status-provider-" + UUID.randomUUID();
        private boolean isStarted = true;
        private boolean isReady = true;
        private boolean isLive = true;

        public Builder withName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder withStarted(boolean isStarted)
        {
            this.isStarted = isStarted;
            return this;
        }

        public Builder withReady(boolean isReady)
        {
            this.isReady = isReady;
            return this;
        }

        public Builder withLive(boolean isLive)
        {
            this.isLive = isLive;
            return this;
        }

        public MockStatusProvider build()
        {
            return new MockStatusProvider(name, isStarted, isReady, isLive);
        }
    }
}
