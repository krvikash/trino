package io.trino.node;

import io.trino.spi.CoordinatorLocator;

import java.net.URI;
import java.util.Set;

public class DefaultCoordinatorLocator
        implements CoordinatorLocator
{
    private final InternalCoordinatorLocator internalCoordinatorLocator;

    public DefaultCoordinatorLocator(InternalCoordinatorLocator internalCoordinatorLocator)
    {
        this.internalCoordinatorLocator = internalCoordinatorLocator;
    }

    @Override
    public Set<URI> getCoordinatorUris()
    {
        return internalCoordinatorLocator.getCoordinatorUris();
    }
}
