/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.execution;

import io.opentelemetry.api.common.Attributes;
import io.starburst.stargate.buffer.data.execution.Exchange.ExchangeResourceUsage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExchangeResourceUsage
{
    @Test
    public void testAttributesReportSpoolingSourceAndWrites()
    {
        ExchangeResourceUsage usage = new ExchangeResourceUsage(2, 3, 40, 15, 25, 30, 60);

        Attributes attributes = usage.toOpentelemetryAttributes(true);

        assertThat(attributes).isEqualTo(Attributes.builder()
                .put("exchange_partitions", 2L)
                .put("exchange_spooled_chunks", 3L)
                .put("exchange_spooled_bytes", 40L)
                .put("exchange_memory_spooled_bytes", 15L)
                .put("exchange_local_disk_spooled_bytes", 25L)
                .put("exchange_bytes_written_to_memory", 30L)
                .put("exchange_bytes_written_to_local_disk", 60L)
                .put("exchange_closed_chunks", 0L)
                .put("exchange_closed_bytes", 0L)
                .build());
    }
}
