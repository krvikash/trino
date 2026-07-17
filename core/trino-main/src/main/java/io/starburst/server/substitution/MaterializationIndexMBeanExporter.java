/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import com.google.inject.Inject;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.MBeanExport;
import org.weakref.jmx.MBeanExporter;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Exports the {@link MaterializationIndex} JMX metrics only while substitution is active. It injects the same
 * {@code Optional<MaterializationIndex>} the read path uses, so on a disabled node the Optional is empty, the
 * index is never built, and nothing is exported.
 */
public class MaterializationIndexMBeanExporter
{
    private final MBeanExporter exporter;
    private final Optional<MaterializationIndex> materializationIndex;
    private MBeanExport export;

    @Inject
    public MaterializationIndexMBeanExporter(MBeanExporter exporter, Optional<MaterializationIndex> materializationIndex)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
        this.materializationIndex = requireNonNull(materializationIndex, "materializationIndex is null");
    }

    @PostConstruct
    public void start()
    {
        materializationIndex.ifPresent(index -> export = exporter.exportWithGeneratedName(index, MaterializationIndex.class));
    }

    @PreDestroy
    public void stop()
    {
        if (export != null) {
            export.unexport();
            export = null;
        }
    }
}
