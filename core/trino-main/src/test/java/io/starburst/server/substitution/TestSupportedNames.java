/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import io.starburst.materialization.ir.SupportedCasts;
import io.starburst.materialization.ir.SupportedCasts.CastPair;
import io.starburst.materialization.ir.SupportedFunctions;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.BooleanOperators.NOT_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SupportedFunctions} and {@link SupportedCasts} spell engine names as literals because their module doesn't
 * depend on trino-main. This test pins each function to the engine constant that names it, and each cast pair to the
 * base names of types the engine casts between.
 */
class TestSupportedNames
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    void testFunctionNamesMatchEngine()
    {
        assertThat(SupportedFunctions.supported()).containsExactlyInAnyOrder(
                builtinFunctionName(EQUAL),
                builtinFunctionName(IDENTICAL),
                builtinFunctionName(LESS_THAN),
                builtinFunctionName(LESS_THAN_OR_EQUAL),
                builtinFunctionName(NOT_FUNCTION_NAME),
                builtinFunctionName(ADD),
                builtinFunctionName(SUBTRACT),
                builtinFunctionName(MULTIPLY),
                builtinFunctionName(DIVIDE));
    }

    @Test
    void testCastBaseNamesMatchEngine()
    {
        assertThat(SupportedCasts.supported()).containsExactlyInAnyOrder(
                resolvedCast(BIGINT, createDecimalType(19)),
                resolvedCast(createDecimalType(10, 2), createDecimalType(12, 2)),
                resolvedCast(INTEGER, BIGINT),
                resolvedCast(INTEGER, createDecimalType(10)),
                resolvedCast(createVarcharType(10), VARCHAR));
    }

    private static CastPair resolvedCast(Type source, Type target)
    {
        assertThat(FUNCTION_RESOLUTION.getCoercion(source, target)).isNotNull();
        return new CastPair(source.getBaseName(), target.getBaseName());
    }
}
