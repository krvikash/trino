/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableSortedSet;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@JsonPropertyOrder(alphabetic = true)
public class License
{
    private final String owner;
    private LicenseType type;
    private Optional<String> hash = Optional.empty();
    private final LocalDateTime expiry;
    // This does not use StarburstPrestoFeature enum so that it is forward compatible
    private final ImmutableSortedSet<String> features;

    @JsonIgnore
    private final String base64Signature;

    public static License unsigned(String owner, LicenseType type, LocalDateTime expiry, ImmutableSortedSet<String> features)
    {
        return new License(owner, type, expiry, features, "");
    }

    public static License unsigned(String owner, LicenseType type, LocalDateTime expiry)
    {
        return new License(owner, type, expiry, ImmutableSortedSet.of(), "");
    }

    @JsonCreator
    public License(
            @JsonProperty("owner") String owner,
            @JsonProperty("type") LicenseType type,
            @JsonProperty("expiry") LocalDateTime expiry,
            @JsonProperty("features") ImmutableSortedSet<String> features,
            @JsonProperty("base64Signature") String base64Signature)
    {
        this.owner = requireNonNull(owner, "owner is null");
        // there is no default value to be backward compatible with previously issues JSON licenses
        this.type = type;
        this.expiry = requireNonNull(expiry, "expiry is null");
        this.features = requireNonNull(features, "features is null");
        this.base64Signature = requireNonNull(base64Signature, "base64Signature is null");
    }

    @JsonGetter("owner")
    public String getOwner()
    {
        return owner;
    }

    @JsonGetter("type")
    public LicenseType getType()
    {
        return type;
    }

    public void setType(LicenseType type)
    {
        this.type = type;
    }

    public Optional<String> getHash()
    {
        return hash;
    }

    public void setHash(String hash)
    {
        this.hash = Optional.of(hash);
    }

    @JsonGetter("expiry")
    public LocalDateTime getExpiry()
    {
        return expiry;
    }

    @JsonGetter("features")
    public ImmutableSortedSet<String> getFeatures()
    {
        return features;
    }

    @JsonIgnore
    public String getBase64Signature()
    {
        return base64Signature;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        License license = (License) o;
        return owner.equals(license.owner) &&
                type.equals(license.type) &&
                expiry.equals(license.expiry) &&
                features.equals(license.features) &&
                base64Signature.equals(license.base64Signature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(owner, type, expiry, features, base64Signature);
    }
}
