/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions.ir;

public record NumberIr(Format format)
        implements LeafIr
{
    public enum Format {
        NONE_NUMBER,
        NONE_INTEGER,
        INT32, // https://spec.openapis.org/registry/format/int32.html
        INT64, // https://spec.openapis.org/registry/format/int64.html
        FLOAT, // https://spec.openapis.org/registry/format/float.html
        DOUBLE, // https://spec.openapis.org/registry/format/double.html
    }
}
