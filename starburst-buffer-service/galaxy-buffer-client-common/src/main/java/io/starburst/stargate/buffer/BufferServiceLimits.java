/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer;

import static com.google.common.base.Preconditions.checkArgument;

public final class BufferServiceLimits
{
    private BufferServiceLimits() {}

    public static final int MAX_TASK_ID = Short.MAX_VALUE;
    public static final int MAX_ATTEMPT_ID = Byte.MAX_VALUE;

    /**
     * @throws IllegalArgumentException if taskId passed as argument is not valid
     */
    public static void validateTaskId(int taskId)
    {
        checkArgument(taskId >= 0 && taskId <= MAX_TASK_ID, "taskId %s larger than %s", taskId, MAX_TASK_ID);
    }

    /**
     * @throws IllegalArgumentException if attemptId passed as argument is not valid
     */
    public static void validateAttemptId(int attemptId)
    {
        checkArgument(attemptId >= 0 && attemptId <= MAX_ATTEMPT_ID, "attemptId %s larger than %s", attemptId, MAX_ATTEMPT_ID);
    }
}
