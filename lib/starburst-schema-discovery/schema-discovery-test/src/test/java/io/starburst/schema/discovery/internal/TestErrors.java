/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestErrors
{
    @Test
    public void testGlobalErrorDeduplicatedInFavorOfTableError()
    {
        Errors errors = new Errors();
        errors.addError("duplicated message");
        errors.addTableError("/some/path", "duplicated message");

        assertThat(errors.buildAll()).containsExactly("[/some/path/] duplicated message");
    }

    @Test
    public void testGlobalErrorWithoutMatchingTableErrorIsRetained()
    {
        Errors errors = new Errors();
        errors.addError("unrelated global message");
        errors.addTableError("/some/path", "table message");

        assertThat(errors.buildAll()).containsExactlyInAnyOrder("unrelated global message", "[/some/path/] table message");
    }

    @Test
    public void testDuplicateTableErrorsAreCollapsed()
    {
        Errors errors = new Errors();
        errors.addTableError("/some/path", "same message");
        errors.addTableError("/some/path", "same message");

        assertThat(errors.buildAll()).containsExactly("[/some/path/] same message");
    }

    @Test
    public void testBuildReturnsOnlyGlobalErrors()
    {
        Errors errors = new Errors();
        errors.addError("global message");
        errors.addTableError("/some/path", "table message");

        assertThat(errors.build()).containsExactly("global message");
    }
}
