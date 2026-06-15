/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AbstractRewriteCast;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.BiFunction;

import static java.sql.Types.BIGINT;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static javax.swing.text.html.parser.DTDConstants.NUMBER;

public class RewriteCast
        extends AbstractRewriteCast
{
    public RewriteCast(BiFunction<ConnectorSession, Type, String> toTargetType)
    {
        super(toTargetType);
    }

    @Override
    protected Optional<JdbcTypeHandle> toJdbcTypeHandle(@SuppressWarnings("unused") ConnectorSession session, JdbcTypeHandle sourceTypeHandle, Type sourceType, Type targetType)
    {
        if (!pushdownSupported(sourceTypeHandle, targetType)) {
            return Optional.empty();
        }

        return switch (targetType) {
            case TinyintType tinyintType -> Optional.of(new JdbcTypeHandle(TINYINT, Optional.of(tinyintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case SmallintType smallintType -> Optional.of(new JdbcTypeHandle(SMALLINT, Optional.of(smallintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case IntegerType integerType -> Optional.of(new JdbcTypeHandle(INTEGER, Optional.of(integerType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case BigintType bigintType -> Optional.of(new JdbcTypeHandle(BIGINT, Optional.of(bigintType.getBaseName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DecimalType decimalType -> Optional.of(new JdbcTypeHandle(
                    NUMBER,
                    Optional.of("NUMBER"),
                    Optional.of(decimalType.getPrecision()),
                    Optional.of(decimalType.getScale()),
                    Optional.empty(),
                    Optional.empty()));
            case VarcharType varcharType -> Optional.of(new JdbcTypeHandle(VARCHAR, Optional.of(varcharType.getBaseName()), varcharType.getLength(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DateType dateType -> Optional.of(new JdbcTypeHandle(DATE, Optional.of(dateType.getBaseName()), Optional.of(10), Optional.empty(), Optional.empty(), Optional.empty()));
            case TimestampType timestampType -> Optional.of(new JdbcTypeHandle(
                    TIMESTAMP,
                    Optional.of(timestampType.getBaseName()),
                    Optional.of(20 + timestampType.getPrecision()),
                    Optional.of(timestampType.getPrecision()),
                    Optional.empty(),
                    Optional.empty()));
            default -> Optional.empty();
        };
    }

    private boolean pushdownSupported(JdbcTypeHandle sourceType, Type targetType)
    {
        return switch (targetType) {
            case TinyintType _, SmallintType _, IntegerType _, BigintType _ -> switch (sourceType.jdbcTypeName().orElse("")) {
                case "NUMBER" -> true;
                default -> false;
            };
            case DecimalType _ -> switch (sourceType.jdbcType()) {
                case TINYINT, SMALLINT, INTEGER, BIGINT -> true;
                default -> switch (sourceType.jdbcTypeName().orElse("")) {
                    case "NUMBER" -> true;
                    default -> false;
                };
            };
            // Casts to char(n) are not pushed down: Trino char values are unpadded after the
            // char/varchar coercion reversal, while Snowflake CHAR-typed results come back
            // space-padded through the parallel reader, yielding values that differ from the
            // engine's own cast
            case VarcharType varcharType -> switch (sourceType.jdbcType()) {
                case CHAR, VARCHAR -> {
                    if (varcharType.isUnbounded()) {
                        yield false;
                    }
                    yield sourceType.jdbcTypeName().map(name -> name.equals("CHAR") || name.equals("VARCHAR")).orElse(false);
                }
                default -> false;
            };
            case DateType _ -> switch (sourceType.jdbcType()) {
                case DATE -> true;
                default -> false;
            };
            case TimestampType timestampType -> switch (sourceType.jdbcType()) {
                case TIMESTAMP -> timestampType.getPrecision() >= sourceType.requiredDecimalDigits();
                default -> false;
            };
            default -> false;
        };
    }
}
