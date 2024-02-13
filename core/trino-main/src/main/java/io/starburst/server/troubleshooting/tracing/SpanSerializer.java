/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting.tracing;

import io.opentelemetry.exporter.internal.otlp.traces.TraceRequestMarshaler;
import io.opentelemetry.sdk.trace.data.SpanData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collection;

public class SpanSerializer
{
    public InputStream execute(Collection<SpanData> spans)
    {
        TraceRequestMarshaler marshaller = TraceRequestMarshaler.create(spans);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            marshaller.writeBinaryTo(os);
            os.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ByteArrayInputStream(os.toByteArray());
    }
}
