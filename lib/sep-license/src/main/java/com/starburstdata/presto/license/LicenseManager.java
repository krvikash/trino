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

import com.google.common.io.ByteSource;
import com.starburstdata.trino.plugin.license.LicenseVerifier;

import java.time.LocalDateTime;
import java.util.Optional;

import static com.starburstdata.presto.license.StarburstFeature.WARP_SPEED;

public interface LicenseManager
        extends LicenseVerifier
{
    boolean hasFeature(StarburstFeature feature);

    void checkFeature(StarburstFeature feature);

    Optional<String> getOwner();

    Optional<LocalDateTime> getExpiry();

    LicenseType getType();

    default String getTier()
    {
        if (hasFeature(WARP_SPEED)) {
            return "Starburst Enterprise Elite";
        }

        return "Starburst Enterprise";
    }

    default Optional<ByteSource> getLicenseFileHandle()
    {
        return Optional.empty();
    }

    default Optional<String> getHash()
    {
        return Optional.empty();
    }
}
