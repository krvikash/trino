/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import static java.util.Objects.requireNonNull;

// Copied from org.apache.commons.lang3.function.TriConsumer
@FunctionalInterface
public interface TriConsumer<T, U, V>
{
    void accept(T k, U v, V s);

    default TriConsumer<T, U, V> andThen(final TriConsumer<? super T, ? super U, ? super V> after)
    {
        requireNonNull(after);
        return (t, u, v) -> {
            accept(t, u, v);
            after.accept(t, u, v);
        };
    }
}
