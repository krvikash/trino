/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.storage.engine.nativeimpl;

import io.airlift.log.Logger;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.spi.TrinoException;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

public class NativeInterruptInterceptor
        implements MethodInterceptor
{
    private static final Logger logger = Logger.get(NativeInterruptInterceptor.class);

    @SuppressWarnings("ThrowFromFinallyBlock")
    @Override
    public Object invoke(MethodInvocation invocation)
            throws Throwable
    {
        try {
            return invocation.proceed();
        }
        catch (TrinoException te) {
            if ((ExceptionThrower.isNativeException(te) || ExceptionThrower.isNativeMatchException(te)) &&
                    Thread.currentThread().isInterrupted()) {
                logger.warn(te, "native execption wad thrown while thread was interrupted");
            }
            throw te;
        }
    }
}
