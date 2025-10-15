/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.trino.client.spooling.SegmentLoader;
import io.trino.client.spooling.SpooledSegment;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

// Prevents early initialization of the actual SegmentLoader until it's needed
public class LazySegmentLoader
        implements SegmentLoader
{
    private final Supplier<SegmentLoader> supplier;

    public LazySegmentLoader(Supplier<SegmentLoader> supplier)
    {
        this.supplier = Suppliers.memoize(requireNonNull(supplier, "supplier is null"));
    }

    @Override
    public InputStream load(SpooledSegment segment)
            throws IOException
    {
        return supplier.get().load(segment);
    }

    @Override
    public void acknowledge(SpooledSegment segment)
            throws IOException
    {
        supplier.get().acknowledge(segment);
    }

    @Override
    public void close()
            throws Exception
    {
        supplier.get().close();
    }
}
