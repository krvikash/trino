/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SapHanaSplit
        extends JdbcSplit
{
    private final Optional<Integer> partitionId;

    @JsonCreator
    public SapHanaSplit(
            @JsonProperty("partitionId") Optional<Integer> partitionId,
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("dynamicFilter") TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        super(additionalPredicate, dynamicFilter);
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
    }

    @JsonProperty
    public Optional<Integer> getPartitionId()
    {
        return partitionId;
    }

    @Override
    public SapHanaSplit withDynamicFilter(TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        return new SapHanaSplit(partitionId, getAdditionalPredicate(), dynamicFilter);
    }
}
