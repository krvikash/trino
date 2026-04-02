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
 * An intermediate representation (IR) to save work transforming schemas of validation keywords to
 * filtered down representations. The intention is to re-use this between parameters and responses.
 */
public sealed interface SchemaIr
        permits ObjectIr,
        ArrayIr,
        JsonIr,
        LeafIr
{
}
