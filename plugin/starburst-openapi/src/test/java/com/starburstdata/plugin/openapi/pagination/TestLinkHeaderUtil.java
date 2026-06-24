/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.pagination;

import com.starburstdata.plugin.openapi.pagination.LinkHeaderUtil.LinkFieldValue;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static com.starburstdata.plugin.openapi.pagination.LinkHeaderUtil.parseLinkFieldValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

/**
 * Port of tests from <a href="https://github.com/joernheissler/httplink/blob/main/tests/test_links.py">httplink.</a>
 */
final class TestLinkHeaderUtil
{
    @Test
    void testEmptyInput()
    {
        assertThat(parseLinkFieldValue("")).isEmpty();
        assertThat(parseLinkFieldValue(",")).isEmpty();
        assertThat(parseLinkFieldValue(" ,, ,,, ,, ")).isEmpty();
    }

    @Test
    void testCompleteExample()
    {
        List<LinkFieldValue> linkFieldValues = parseLinkFieldValue("<http://example.com/TheBook/chapter2>; title=\"previous chapter\"; rel=previous, <http://example.com/TheBook/chapter4>; rel=next; title=\"next chapter\"");
        assertThat(linkFieldValues).hasSize(2);
        LinkFieldValue previousChapterLink = linkFieldValues.get(0);
        assertThat(previousChapterLink.uri()).isEqualTo(URI.create("http://example.com/TheBook/chapter2"));
        assertThat(previousChapterLink.parameters()).hasSize(2);
        assertThat(previousChapterLink.parameters()).containsEntry("rel", "previous");
        assertThat(previousChapterLink.parameters()).containsEntry("title", "previous chapter");
        LinkFieldValue nextChapterLink = linkFieldValues.get(1);
        assertThat(nextChapterLink.uri()).isEqualTo(URI.create("http://example.com/TheBook/chapter4"));
        assertThat(nextChapterLink.parameters()).hasSize(2);
        assertThat(nextChapterLink.parameters()).containsEntry("rel", "next");
        assertThat(nextChapterLink.parameters()).containsEntry("title", "next chapter");
    }

    @Test
    void testRelativeExample()
    {
        assertThat(parseLinkFieldValue("</>;"))
                .singleElement()
                .extracting(LinkFieldValue::uri)
                .isEqualTo(URI.create("/"));
    }

    @Test
    void testNoParameters()
    {
        assertThat(parseLinkFieldValue("<http://example.org/no_parameters>"))
                .singleElement()
                .extracting(LinkFieldValue::parameters)
                .asInstanceOf(map(String.class, String.class))
                .isEmpty();
    }

    @Test
    void testEmptyParameterValue()
    {
        assertThat(parseLinkFieldValue("<http://example.org/no_parameters>; key"))
                .singleElement()
                .extracting(LinkFieldValue::parameters)
                .asInstanceOf(map(String.class, String.class))
                .containsOnly(entry("key", ""));
    }

    @Test
    void testQuotedValue()
    {
        assertThat(parseLinkFieldValue("<https://example.org>; title=\"\\r\\n \\\\\""))
                .singleElement()
                .extracting(LinkFieldValue::parameters)
                .asInstanceOf(map(String.class, String.class))
                .containsExactly(entry("title", "rn \\"));
    }

    @Test
    void testExtendedValue()
    {
        // Allowed by specification but excluded for simplicity.
        assertThatThrownBy(() ->
                parseLinkFieldValue("<https://example.org>; title*=UTF-8'de'n%c3%a4chstes%20Kapitel"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testWhitespace()
    {
        // Uses lots of whitespace including whitespace the specification marks as bad: not advised but impl handles.
        assertThat(parseLinkFieldValue("<https://example.org> \t ; \t key \t = \t \"value\""))
                .singleElement()
                .extracting(LinkFieldValue::parameters)
                .asInstanceOf(map(String.class, String.class))
                .containsExactly(entry("key", "value"));
    }
}
