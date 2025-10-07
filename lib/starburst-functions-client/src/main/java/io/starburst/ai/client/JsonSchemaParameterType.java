/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

/*
 * Reference: https://json-schema.org/understanding-json-schema/reference/type
 */

import static java.util.Locale.ENGLISH;

public enum JsonSchemaParameterType
{
    STRING,
    NUMBER,
    INTEGER,
    BOOLEAN,
    OBJECT,
    ARRAY;

    @Override
    public String toString()
    {
        return name().toLowerCase(ENGLISH);
    }
}
