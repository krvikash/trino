/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.ai;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.security.AiModelAccessControl;

import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class CachingAiModelAccessControl
        implements AiModelAccessControl
{
    private final LoadingCache<CacheKey, Boolean> accessControlCache;

    public CachingAiModelAccessControl(AiModelAccessControl delegate)
    {
        requireNonNull(delegate, "delegate is null");
        this.accessControlCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(4, HOURS)
                .maximumSize(5000)
                .build(CacheLoader.from(key -> {
                    delegate.checkCanExecuteModel(key.context(), key.modelId());
                    return true;
                }));
    }

    @Override
    public void checkCanExecuteModel(Context context, String modelId)
    {
        try {
            if (!accessControlCache.get(new CacheKey(context, modelId))) {
                denyExecuteAiModelAccess(modelId);
            }
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }
    }

    private record CacheKey(Context context, String modelId) {}
}
