/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

/**
 * Used for determining whether discovered identifiers needs to follow additional constraints (f.e. when using HMS)
 * <p>
 * When used only for deserialization of already discovered tables, VALID_IN_TRINO should be used as it allows more identifiers
 */
public enum IdentifierConstraint
{
    VALID_IN_TRINO,
    VALID_IN_HIVE_AND_TRINO
}
