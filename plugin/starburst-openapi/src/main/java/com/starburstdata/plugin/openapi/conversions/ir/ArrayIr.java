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

/**
 * Represents a value validated as a JSON array.
 * <p>
 * This also sees re-use in the openAPI specification to represent a list of values for a parameter.
 *
 * @param items The type of the items contained in this array.
 */
public record ArrayIr(SchemaIr items)
        implements SchemaIr
{
}
