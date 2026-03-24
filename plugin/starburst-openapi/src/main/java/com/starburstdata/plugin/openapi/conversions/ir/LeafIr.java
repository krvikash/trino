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
 * An interface of intermediate representations separated out to values that don't contain other SchemaIr's.
 */
public sealed interface LeafIr
        extends SchemaIr
        permits BooleanIr,
        JsonIr,
        StringIr,
        NumberIr
{
}
