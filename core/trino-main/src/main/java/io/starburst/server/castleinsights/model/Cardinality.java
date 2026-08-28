/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.model;

/**
 * The relationship between the two sides of a join, from {@code left} to {@code right}. Absent where
 * it could not be determined, so it is held as an {@code Optional} rather than carrying a sentinel.
 * <p>
 * {@code ONE_TO_MANY} is reserved: telling it apart from {@code MANY_TO_MANY} needs the probe side's
 * uniqueness, which the engine does not measure cheaply.
 */
public enum Cardinality
{
    MANY_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_MANY,
}
