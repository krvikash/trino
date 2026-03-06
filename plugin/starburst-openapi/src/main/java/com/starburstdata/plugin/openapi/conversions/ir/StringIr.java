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

public record StringIr(Format format)
        implements LeafIr
{
    public enum Format {
        NONE,
        BYTE, // B64 https://spec.openapis.org/registry/format/byte.html
        UUID, // https://spec.openapis.org/registry/format/uuid.html
    }
}
