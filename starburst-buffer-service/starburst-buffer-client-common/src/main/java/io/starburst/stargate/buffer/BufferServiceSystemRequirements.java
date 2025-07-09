/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer;

import com.google.errorprone.annotations.FormatMethod;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.regex.Pattern.quote;

// FIXME the class does not belong to client-common module. Put there opportunistically. TODO move to a (new) appropriate module.
public final class BufferServiceSystemRequirements
{
    private BufferServiceSystemRequirements() {}

    public static void verifySystemRequirements()
    {
        verifyJdk8329528Workaround();
    }

    private static void verifyJdk8329528Workaround()
    {
        if (Runtime.version().feature() < 22) {
            // Upstream requires 22.0.1, but Galaxy Trino temporarily downgrades minimum required Java version for runtime
            return;
        }
        if (Runtime.version().compareTo(Runtime.Version.parse("22.0.2")) < 0) {
            Optional<String> collectionsKeepPinned = getJvmConfigurationFlag("XX:G1NumCollectionsKeepPinned");
            int requiredValue = 10000000;
            if (collectionsKeepPinned.isEmpty() || parseInt(collectionsKeepPinned.get()) < requiredValue) {
                failRequirement("Trino requires -XX:+UnlockDiagnosticVMOptions -XX:G1NumCollectionsKeepPinned=%d on Java versions lower than 22.0.2 due to JDK-8329528", requiredValue);
            }
        }
    }

    private static Optional<String> getJvmConfigurationFlag(String flag)
    {
        Pattern pattern = Pattern.compile("-%s=(.*)".formatted(quote(flag)), Pattern.DOTALL);
        Optional<String> matched = Optional.empty();
        List<String> matching = new ArrayList<>(1);
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            Matcher matcher = pattern.matcher(argument);
            if (matcher.matches()) {
                matched = Optional.of(matcher.group(1));
                matching.add(argument);
            }
        }
        if (matching.size() > 1) {
            failRequirement("Multiple JVM configuration flags matched %s: %s", pattern.pattern(), matching);
        }
        return matched;
    }

    @FormatMethod
    private static void failRequirement(String format, Object... args)
    {
        System.err.println("ERROR: " + format(format, args));
        System.exit(100);
    }
}
