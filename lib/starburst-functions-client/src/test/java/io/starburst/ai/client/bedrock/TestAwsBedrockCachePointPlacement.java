/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import com.google.common.collect.ImmutableList;
import io.starburst.ai.client.LlmMessage;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.bedrockruntime.model.CachePointType;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.SystemContentBlock;

import java.util.ArrayList;
import java.util.List;

import static io.starburst.ai.client.MessageRole.ASSISTANT;
import static io.starburst.ai.client.MessageRole.USER;
import static io.starburst.ai.client.bedrock.AwsBedrockLanguageModelClient.MIN_CACHE_POINT_CHARS;
import static io.starburst.ai.client.bedrock.AwsBedrockLanguageModelClient.addSystemCachePoint;
import static io.starburst.ai.client.bedrock.AwsBedrockLanguageModelClient.buildMessagesWithCachePoint;
import static org.assertj.core.api.Assertions.assertThat;

class TestAwsBedrockCachePointPlacement
{
    private static final int INC = MIN_CACHE_POINT_CHARS;

    @Test
    void testNoSystemCachePointWhenBelowMinimum()
    {
        List<SystemContentBlock> blocks = ImmutableList.of(
                SystemContentBlock.fromText("short prompt"));
        List<SystemContentBlock> result = addSystemCachePoint(blocks, true);
        assertThat(result).isEqualTo(blocks);
    }

    @Test
    void testSystemCachePointAddedWhenAboveMinimum()
    {
        String longPrompt = "x".repeat(MIN_CACHE_POINT_CHARS);
        List<SystemContentBlock> blocks = ImmutableList.of(SystemContentBlock.fromText(longPrompt));
        List<SystemContentBlock> result = addSystemCachePoint(blocks, true);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).text()).isEqualTo(longPrompt);
        assertThat(result.get(1).cachePoint().type()).isEqualTo(CachePointType.DEFAULT);
    }

    @Test
    void testNoSystemCachePointWhenCachingDisabled()
    {
        String longPrompt = "x".repeat(MIN_CACHE_POINT_CHARS);
        List<SystemContentBlock> blocks = ImmutableList.of(SystemContentBlock.fromText(longPrompt));
        List<SystemContentBlock> result = addSystemCachePoint(blocks, false);
        assertThat(result).isEqualTo(blocks);
    }

    @Test
    void testSystemCachePointWithMultipleBlocks()
    {
        String half = "x".repeat(MIN_CACHE_POINT_CHARS / 2);
        List<SystemContentBlock> blocks = ImmutableList.of(
                SystemContentBlock.fromText(half),
                SystemContentBlock.fromText(half));
        List<SystemContentBlock> result = addSystemCachePoint(blocks, true);
        assertThat(result).hasSize(3);
        assertThat(result.get(2).cachePoint().type()).isEqualTo(CachePointType.DEFAULT);
    }

    @Test
    void testEmptyMessages()
    {
        List<Message> result = buildMessagesWithCachePoint(ImmutableList.of(), ImmutableList.of(), true);
        assertThat(result).isEmpty();
    }

    @Test
    void testNoCachePointWhenBelowOneIncrement()
    {
        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "x".repeat(INC - 1)));
        List<Message> result = buildMessagesWithCachePoint(messages, ImmutableList.of(), true);
        assertThat(countCachePoints(result)).isEqualTo(0);
    }

    @Test
    void testOnlyCp2WhenBetweenOneAndTwoIncrements()
    {
        List<LlmMessage> messages = buildConversation(INC + INC / 2);
        List<Message> result = buildMessagesWithCachePoint(messages, ImmutableList.of(), true);
        assertThat(countCachePoints(result)).isEqualTo(1);
    }

    @Test
    void testNoCachePointWhenCachingDisabled()
    {
        List<LlmMessage> messages = buildConversation(INC + INC / 2);
        List<Message> result = buildMessagesWithCachePoint(messages, ImmutableList.of(), false);
        assertThat(countCachePoints(result)).isEqualTo(0);
    }

    @Test
    void testBothCp2AndCp3WhenAtTwoIncrements()
    {
        List<LlmMessage> messages = buildConversation(2 * INC + INC / 2);
        List<Message> result = buildMessagesWithCachePoint(messages, ImmutableList.of(), true);
        List<Integer> cpIndices = findAllCachePointIndices(result);
        assertThat(cpIndices).hasSize(2);
        assertThat(cpIndices.get(0)).isLessThan(cpIndices.get(1));
    }

    @Test
    void testCachePointsStableWithinSlot()
    {
        // CP2 at 1*INC, CP3 at 2*INC. Stable while the stable prefix stays in [2*INC, 3*INC).
        // Use 2*INC + 1500 chars (23 messages ending USER): stable prefix = 21 messages = 10500 chars.
        List<LlmMessage> conversation = buildConversation(2 * INC + 1500);
        List<Message> result1 = buildMessagesWithCachePoint(conversation, ImmutableList.of(), true);
        List<Integer> indices1 = findAllCachePointIndices(result1);
        assertThat(indices1).hasSize(2);

        // Add one more [ASSISTANT, USER] turn. The stable prefix of the grown conversation
        // equals the full previous conversation, still well within [2*INC, 3*INC).
        List<LlmMessage> grown = ImmutableList.<LlmMessage>builder()
                .addAll(conversation)
                .add(new LlmMessage(ASSISTANT, "x".repeat(500)))
                .add(new LlmMessage(USER, "x".repeat(500)))
                .build();
        assertThat(totalChars(grown)).isLessThan(3 * INC);
        List<Message> result2 = buildMessagesWithCachePoint(grown, ImmutableList.of(), true);
        List<Integer> indices2 = findAllCachePointIndices(result2);

        // Same message indices → cache hits on the next turn
        assertThat(indices2).isEqualTo(indices1);
    }

    @Test
    void testCachePointsAdvanceAtSlideThreshold()
    {
        // Use ASSISTANT-ending conversations (multiple of 1000 chars) so stable prefix = full
        // message list and the threshold comparison is unambiguous.
        // Before slide: stable prefix just under 3*INC → CP2 at 1*INC, CP3 at 2*INC
        List<LlmMessage> beforeSlide = buildConversation(3 * INC - 1000);
        List<Message> resultBefore = buildMessagesWithCachePoint(beforeSlide, ImmutableList.of(), true);
        List<Integer> indicesBefore = findAllCachePointIndices(resultBefore);

        // After slide: stable prefix >= 3*INC → CP2 at 2*INC, CP3 at 3*INC
        List<LlmMessage> afterSlide = buildConversation(3 * INC + 1000);
        List<Message> resultAfter = buildMessagesWithCachePoint(afterSlide, ImmutableList.of(), true);
        List<Integer> indicesAfter = findAllCachePointIndices(resultAfter);

        // Both CPs should have advanced
        assertThat(indicesAfter.get(0)).isGreaterThan(indicesBefore.get(0));
        assertThat(indicesAfter.get(1)).isGreaterThan(indicesBefore.get(1));
    }

    @Test
    void testCp2TakesCp3OldPosition()
    {
        // Use ASSISTANT-ending conversations so stable prefix = full message list.
        // Before slide: CP3 is at the 2*INC boundary
        List<LlmMessage> beforeSlide = buildConversation(3 * INC - 1000);
        List<Message> resultBefore = buildMessagesWithCachePoint(beforeSlide, ImmutableList.of(), true);
        List<Integer> indicesBefore = findAllCachePointIndices(resultBefore);
        int cp3BeforeIndex = indicesBefore.get(1);

        // After slide: CP2 should be at CP3's old position (or very close)
        List<LlmMessage> afterSlide = buildConversation(3 * INC + 1000);
        List<Message> resultAfter = buildMessagesWithCachePoint(afterSlide, ImmutableList.of(), true);
        List<Integer> indicesAfter = findAllCachePointIndices(resultAfter);
        int cp2AfterIndex = indicesAfter.getFirst();

        // CP2's new position should match CP3's old position
        assertThat(cp2AfterIndex).isGreaterThanOrEqualTo(cp3BeforeIndex);
    }

    @Test
    void testMultipleSlides()
    {
        // Track CP positions across multiple slides
        int previousCp2 = -1;
        int slideCount = 0;

        for (int totalChars = 2 * INC; totalChars <= 6 * INC; totalChars += INC / 2) {
            List<LlmMessage> messages = buildConversation(totalChars);
            List<Message> result = buildMessagesWithCachePoint(messages, ImmutableList.of(), true);
            List<Integer> indices = findAllCachePointIndices(result);

            if (!indices.isEmpty() && indices.getFirst() != previousCp2) {
                if (previousCp2 >= 0) {
                    slideCount++;
                }
                previousCp2 = indices.getFirst();
            }
        }

        // Should have slid multiple times across 2*INC to 6*INC range
        assertThat(slideCount).isGreaterThanOrEqualTo(2);
    }

    @Test
    void testSystemCpOffsetAffectsMessageCpPlacement()
    {
        String longPrompt = "x".repeat(MIN_CACHE_POINT_CHARS);
        List<SystemContentBlock> blocks = ImmutableList.of(SystemContentBlock.fromText(longPrompt));
        List<SystemContentBlock> system = addSystemCachePoint(blocks, true);
        // Messages with INC chars → charsAfterCachePoint = INC → only CP2
        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "x".repeat(INC)));
        List<Message> result = buildMessagesWithCachePoint(messages, system, true);
        assertThat(countCachePoints(result)).isEqualTo(1);
    }

    @Test
    void testCachePointBlockStructure()
    {
        List<LlmMessage> messages = buildConversation(2 * INC + 100);
        List<Message> result = buildMessagesWithCachePoint(messages, ImmutableList.of(), true);

        for (Message message : result) {
            if (hasCachePoint(message)) {
                List<ContentBlock> blocks = message.content();
                assertThat(blocks).hasSize(2);
                assertThat(blocks.get(0).text()).isNotNull();
                assertThat(blocks.get(1).cachePoint().type()).isEqualTo(CachePointType.DEFAULT);
            }
        }
    }

    @Test
    void testSingleLargeMessageCollapsesToOneCachePoint()
    {
        // One message spans multiple increments — CP2 and CP3 resolve to same index
        List<LlmMessage> messages = ImmutableList.of(
                new LlmMessage(USER, "x".repeat(3 * INC)));
        List<Message> result = buildMessagesWithCachePoint(messages, ImmutableList.of(), true);
        // Should place only one cache point (both targets resolve to the only message)
        assertThat(countCachePoints(result)).isEqualTo(1);
    }

    @Test
    void testTransitionFromCp2OnlyToBothCps()
    {
        // Use ASSISTANT-ending conversations (multiples of 1000 chars) so stable prefix = full
        // message list and both sides of the transition are computed consistently.
        // Start with only CP2
        List<LlmMessage> onlyCp2 = buildConversation(INC + 1000);
        List<Message> result1 = buildMessagesWithCachePoint(onlyCp2, ImmutableList.of(), true);
        assertThat(countCachePoints(result1)).isEqualTo(1);
        int cp2Index = findAllCachePointIndices(result1).getFirst();

        // Grow to have both CP2 and CP3
        List<LlmMessage> bothCps = buildConversation(2 * INC + 1000);
        List<Message> result2 = buildMessagesWithCachePoint(bothCps, ImmutableList.of(), true);
        assertThat(countCachePoints(result2)).isEqualTo(2);

        // CP2 should stay at the same position
        int cp2IndexAfter = findAllCachePointIndices(result2).getFirst();
        assertThat(cp2IndexAfter).isEqualTo(cp2Index);
    }

    @Test
    void testSimulatedMultiTurnStability()
    {
        // Simulate turns with ~1K per message, verify stability between slides
        String longPrompt = "x".repeat(5000);
        List<SystemContentBlock> blocks = ImmutableList.of(SystemContentBlock.fromText(longPrompt));
        List<SystemContentBlock> system = addSystemCachePoint(blocks, true);
        ImmutableList.Builder<LlmMessage> conversationBuilder = ImmutableList.builder();
        List<Integer> previousIndices = List.of();
        int stableTurns = 0;

        for (int turn = 0; turn < 20; turn++) {
            conversationBuilder.add(new LlmMessage(
                    turn % 2 == 0 ? USER : ASSISTANT,
                    "x".repeat(1000)));
            List<LlmMessage> conversation = conversationBuilder.build();
            List<Message> result = buildMessagesWithCachePoint(conversation, system, true);
            List<Integer> indices = findAllCachePointIndices(result);

            if (indices.equals(previousIndices) && !indices.isEmpty()) {
                stableTurns++;
            }
            previousIndices = indices;
        }

        // Should be stable for multiple consecutive turns between slides
        assertThat(stableTurns).isGreaterThanOrEqualTo(5);
    }

    @Test
    void testLargeLastAssistantMessageDoesNotBustCachePoints()
    {
        // Regression test for the flaw where a large assistant response caused CP2 to slide
        // past itself, busting the cache on the very next user turn.
        //
        // Call A: conversation ending in USER → 1 CP placed at the INC boundary.
        List<LlmMessage> callA = buildConversation(INC + INC / 2); // 15 msgs, ends USER
        List<Integer> indicesA = findAllCachePointIndices(
                buildMessagesWithCachePoint(callA, ImmutableList.of(), true));
        assertThat(indicesA).hasSize(1);

        // Call B: model replies with a HUGE response, then user sends the next message.
        List<LlmMessage> callB = ImmutableList.<LlmMessage>builder()
                .addAll(callA)
                .add(new LlmMessage(ASSISTANT, "x".repeat(5 * INC))) // huge response
                .add(new LlmMessage(USER, "x".repeat(500)))
                .build();
        // With simulation: the previous turn boundary (callA ending in USER) determines CP2's
        // position. callA's chars are still in [INC, 2*INC) so CP2 target is unchanged.
        // CP3 may also appear (total content is now huge, which is fine — more caching, not less).
        List<Integer> indicesB = findAllCachePointIndices(
                buildMessagesWithCachePoint(callB, ImmutableList.of(), true));
        assertThat(indicesB).isNotEmpty();
        assertThat(indicesB.getFirst())
                .as("CP2 must not move after a large assistant response — cache would be busted")
                .isEqualTo(indicesA.getFirst());
    }

    /**
     * Builds a conversation of approximately the given total characters
     * using many small messages to provide fine-grained message boundaries.
     */
    private static List<LlmMessage> buildConversation(int totalChars)
    {
        int messageSize = 500;
        ImmutableList.Builder<LlmMessage> builder = ImmutableList.builder();
        int remaining = totalChars;
        int index = 0;
        while (remaining > 0) {
            int size = Math.min(messageSize, remaining);
            // alternate USER and ASSISTANT, but make sure we end with USER
            builder.add(new LlmMessage(index % 2 == 0 || size == remaining ? USER : ASSISTANT, "x".repeat(size)));
            remaining -= size;
            index++;
        }
        return builder.build();
    }

    private static int totalChars(List<LlmMessage> messages)
    {
        return messages.stream().mapToInt(m -> m.content().length()).sum();
    }

    private static boolean hasCachePoint(Message message)
    {
        return message.content().stream()
                .anyMatch(block -> block.cachePoint() != null);
    }

    private static int countCachePoints(List<Message> messages)
    {
        return (int) messages.stream().filter(TestAwsBedrockCachePointPlacement::hasCachePoint).count();
    }

    private static List<Integer> findAllCachePointIndices(List<Message> messages)
    {
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            if (hasCachePoint(messages.get(i))) {
                indices.add(i);
            }
        }
        return indices;
    }
}
