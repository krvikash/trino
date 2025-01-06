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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLowerCaseString
{
    @Test
    public void testLowerCaseString()
    {
        LowerCaseString lowerCaseString = LowerCaseString.toLowerCase("fooBar");
        assertThat(lowerCaseString.string()).isEqualTo("foobar");
        assertThat(lowerCaseString.toString()).isEqualTo("foobar");
        assertThat(lowerCaseString.getOriginalString()).isEqualTo("fooBar");

        LowerCaseString equavilentLowerCaseString = new LowerCaseString("foobar");
        assertThat(lowerCaseString).isEqualTo(equavilentLowerCaseString);
        assertThat(lowerCaseString.hashCode()).isEqualTo(equavilentLowerCaseString.hashCode());
    }
}
