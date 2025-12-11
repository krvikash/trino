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

import java.io.PrintWriter;
import java.io.StringWriter;

public final class ExceptionUtils
{
    private ExceptionUtils() {}

    // Copied from org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace
    public static String getStackTrace(Throwable throwable)
    {
        if (throwable == null) {
            return "";
        }
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer, true));
        return writer.toString();
    }
}
