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

import com.google.common.base.CharMatcher;

import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;

// based on https://github.com/apache/hive/blob/master/hplsql/src/main/antlr4/org/apache/hive/hplsql/Hplsql.g4#L1988
record HiveIdentifier(LowerCaseString name)
        implements DiscoveredIdentifier
{
    private static final CharMatcher MINUS_SIGN_MATCHER = CharMatcher.is('-')
            .precomputed();
    private static final CharMatcher ASCII_LETTERS_MATCHER = CharMatcher.inRange('a', 'z')
            .or(CharMatcher.inRange('A', 'Z'))
            .precomputed();
    private static final CharMatcher HIVE_SPECIAL_SIGNS_MATCHER = CharMatcher.anyOf("_@:#$")
            .precomputed();
    private static final CharMatcher REGULAR_CHARS_MATCHER = CharMatcher.inRange('a', 'z')
            .or(CharMatcher.inRange('A', 'Z'))
            .or(CharMatcher.is('_'))
            .or(CharMatcher.inRange('0', '9'))
            .precomputed();
    private static final CharMatcher AFTER_SPECIAL_MATCHER = REGULAR_CHARS_MATCHER
            .or(HIVE_SPECIAL_SIGNS_MATCHER)
            .precomputed();
    private static final CharMatcher ALL_HIVE_CHARACTERS = AFTER_SPECIAL_MATCHER
            .or(MINUS_SIGN_MATCHER)
            .precomputed();

    HiveIdentifier
    {
        checkArgument(isValidName(name.getOriginalString()), "Identifier: %s is not valid for Hive compatibility", name);
    }

    HiveIdentifier(String name)
    {
        this(toLowerCase(name));
    }

    @Override
    public String string()
    {
        return name.string();
    }

    @Override
    public String toString()
    {
        return string();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null) {
            return false;
        }
        if (o instanceof DiscoveredIdentifier that) {
            return Objects.equals(this.string(), that.string());
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    public static HiveIdentifier toHiveIdentifier(String name)
    {
        return toHiveIdentifier(toLowerCase(name));
    }

    public static HiveIdentifier toHiveIdentifier(LowerCaseString name)
    {
        return convertToValidHiveIdentifier(name);
    }

    private static HiveIdentifier convertToValidHiveIdentifier(LowerCaseString name)
    {
        if (isValidName(name.getOriginalString())) {
            return new HiveIdentifier(name);
        }

        if (name.getOriginalString().isBlank()) {
            throw new IllegalArgumentException("name cannot be blank");
        }
        String fixedName = name.getOriginalString().codePoints()
                .mapToObj(c -> String.valueOf((char) c))
                .filter(ALL_HIVE_CHARACTERS::matchesAllOf)
                .collect(Collectors.joining());
        // try to remove first character until we have name that is valid
        while (fixedName.length() > 1 && !isValidName(fixedName)) {
            fixedName = fixedName.substring(1);
        }
        // return original name, as at this point fixedName has 1 length
        // which is not expected outcome, and with original name in error it's easier to
        // interpret error message
        if (fixedName.length() == 1) {
            return new HiveIdentifier(name);
        }
        return new HiveIdentifier(fixedName);
    }

    private static boolean isValidName(String name)
    {
        String stringBeingValidated = name;
        if (stringBeingValidated.isBlank()) {
            return false;
        }
        char firstChar = stringBeingValidated.charAt(0);
        // do not consider minus in further validations, start after it
        if (MINUS_SIGN_MATCHER.matches(firstChar)) {
            if (stringBeingValidated.length() == 1) {
                return true;
            }
            stringBeingValidated = stringBeingValidated.substring(1);
            firstChar = stringBeingValidated.charAt(0);
        }

        // table starts with - or regular letter, validate regular name
        if (ASCII_LETTERS_MATCHER.matches(firstChar)) {
            return stringBeingValidated.length() == 1 || REGULAR_CHARS_MATCHER.matchesAllOf(stringBeingValidated.substring(1));
        }

        // table starts with special character, allow following special characters
        if (HIVE_SPECIAL_SIGNS_MATCHER.matches(firstChar)) {
            return stringBeingValidated.length() > 1 && AFTER_SPECIAL_MATCHER.matchesAllOf(stringBeingValidated.substring(1));
        }
        return false;
    }
}
