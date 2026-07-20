/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import io.airlift.log.Logger;
import io.starburst.schema.discovery.models.TablePath;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;

public class Errors
{
    private static final Logger log = Logger.get(Errors.class);

    private final Set<String> errors = Sets.newConcurrentHashSet();
    private final Map<String, Set<String>> tablePathToTableErrors = new ConcurrentHashMap<>();
    // files (longer paths) before their parent directories, then alphabetically by path to break ties deterministically
    // (both sets backing this class are unordered, so without this tiebreak the output order depends on hash iteration)
    private static final Comparator<Map.Entry<String, Set<String>>> FILES_BEFORE_PARENTS_COMPARATOR = Comparator
            .<Map.Entry<String, Set<String>>>comparingInt(o -> o.getKey().length()).reversed()
            .thenComparing(Map.Entry::getKey);

    @FormatMethod
    public void addTableError(String withinTablePath, @FormatString String error, Object... args)
    {
        String message = String.format(error, args);
        tablePathToTableErrors.computeIfAbsent(ensureEndsWithSlash(withinTablePath).toString(), _ -> Sets.newConcurrentHashSet())
                .add(message);
        log.warn("Schema discovery table error at [%s]: %s", withinTablePath, message);
    }

    @FormatMethod
    public void addTableError(TablePath withinTablePath, @FormatString String error, Object... args)
    {
        String message = String.format(error, args);
        tablePathToTableErrors.computeIfAbsent(ensureEndsWithSlash(withinTablePath.path()).toString(), _ -> Sets.newConcurrentHashSet())
                .add(message);
        log.warn("Schema discovery table error at [%s]: %s", withinTablePath.path(), message);
    }

    /**
     * Records a schema-level error that cannot be associated with any particular table path
     * (e.g. a condition affecting the whole discovery root).
     */
    @FormatMethod
    public void addError(@FormatString String error, Object... args)
    {
        String message = String.format(error, args);
        errors.add(message);
        log.warn("Schema discovery error: %s", message);
    }

    public List<String> build()
    {
        return ImmutableList.copyOf(errors);
    }

    /**
     * Returns all collected errors (both schema-level and per-table), suitable for surfacing
     * in the {@code errors} column of the schema discovery system table. A schema-level error
     * is dropped if the same message already appears as a table error, so that the located
     * (path-prefixed) copy takes precedence over the bare one.
     */
    public List<String> buildAll()
    {
        Set<String> tableErrorMessages = tablePathToTableErrors.values().stream()
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        return Stream.concat(
                        errors.stream().filter(error -> !tableErrorMessages.contains(error)).sorted(),
                        tablePathToTableErrors.entrySet().stream()
                                .sorted(FILES_BEFORE_PARENTS_COMPARATOR)
                                .flatMap(entry -> entry.getValue().stream()
                                        .sorted()
                                        .map(message -> prefixWithPath(entry.getKey(), message))))
                .distinct()
                .collect(toImmutableList());
    }

    public List<String> buildForPathAndChildren(String path)
    {
        String slashEndedBasePath = ensureEndsWithSlash(path).toString();
        return tablePathToTableErrors.entrySet().stream()
                .filter(e -> e.getKey().startsWith(slashEndedBasePath))
                .sorted(FILES_BEFORE_PARENTS_COMPARATOR)
                .flatMap(entry -> entry.getValue().stream()
                        .sorted()
                        .map(message -> prefixWithPath(entry.getKey(), message)))
                .distinct()
                .collect(toImmutableList());
    }

    private static String prefixWithPath(String path, String message)
    {
        String pathWithoutTrailingSlash = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return message.contains(pathWithoutTrailingSlash) ? message : "[%s] %s".formatted(path, message);
    }

    public Map<String, List<String>> buildPathErrors()
    {
        return tablePathToTableErrors.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
    }
}
