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

import java.util.Map;
import java.util.Optional;

/**
 * Represents a value validated as a JSON object.
 *
 * @param properties Known named properties of this JSON object associated to their type.
 * @param additionalProperties If present the type of extra properties (unknown names),
 * if empty no extra properties expected.
 */
public record ObjectIr(
        Map<String, SchemaIr> properties,
        Optional<SchemaIr> additionalProperties)
        implements SchemaIr
{
}
