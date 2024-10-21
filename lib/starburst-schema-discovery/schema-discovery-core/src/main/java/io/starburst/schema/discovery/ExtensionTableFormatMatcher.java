/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import io.starburst.schema.discovery.models.TableFormat;
import io.trino.filesystem.Location;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Map.entry;

public class ExtensionTableFormatMatcher
{
    private final Map<String, TableFormat> extensionBasedTableFormats;

    public ExtensionTableFormatMatcher(Set<TableFormat> tableFormats)
    {
        extensionBasedTableFormats = tableFormats.stream()
                .filter(TableFormat::canBeDeterminedByFileExtension)
                .flatMap(format -> format.getExtensions().stream()
                        .map(extension -> entry(extension, format)))
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    public TableFormat match(Location location)
    {
        return Optional.ofNullable(getFileExtension(location))
                .map(extensionBasedTableFormats::get)
                .orElse(TableFormat.UNKNOWN);
    }

    private String getFileExtension(Location filePath)
    {
        String fileName = filePath.fileName();
        int position = fileName.lastIndexOf(46);
        if (position < 0) {
            return null;
        }
        return fileName.substring(position);
    }
}
