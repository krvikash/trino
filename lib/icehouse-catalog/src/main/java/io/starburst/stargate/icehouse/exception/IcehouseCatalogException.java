/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.starburst.stargate.icehouse.exception;

import com.google.common.base.Throwables;

/**
 * Base exception for errors encountered during metastore discovery operations
 * (e.g., listing schemas and tables). Subclasses must classify themselves as
 * retryable or permanent via {@link #isRetryable()}.
 */
public abstract class IcehouseCatalogException
        extends RuntimeException
{
    protected IcehouseCatalogException(String message, Throwable cause)
    {
        super(message, cause);
    }

    protected IcehouseCatalogException(String message)
    {
        super(message);
    }

    /**
     * Returns {@code true} if this error is retryable and the operation should be retried
     * indefinitely (e.g., network timeouts, throttling, temporary unavailability).
     * Returns {@code false} if this error is permanent and the entity may eventually be
     * unregistered after a timeout (e.g., access denied, entity not found).
     */
    public abstract boolean isRetryable();

    /**
     * Walks the exception cause chain looking for an {@link IcehouseCatalogException} whose
     * {@link #isRetryable()} returns {@code true}. Useful when a retryable catalog error has
     * been wrapped (e.g., in a {@link RuntimeException}) by an intermediate layer.
     */
    public static boolean isRetryableInChain(Throwable throwable)
    {
        return Throwables.getCausalChain(throwable).stream()
                .anyMatch(cause -> cause instanceof IcehouseCatalogException catalogException && catalogException.isRetryable());
    }
}
