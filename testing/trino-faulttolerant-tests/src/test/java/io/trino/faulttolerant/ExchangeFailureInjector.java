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
package io.trino.faulttolerant;

import io.trino.spi.exchange.ExchangeId;

import java.util.HashSet;
import java.util.Set;

public class ExchangeFailureInjector
{
    private int failuresRemaining;
    private final Set<ExchangeId> lostExchanges = new HashSet<>();

    public void injectFailure()
    {
        injectFailures(1);
    }

    public synchronized void injectFailures(int count)
    {
        failuresRemaining += count;
    }

    public synchronized void stopInjectingFailures()
    {
        failuresRemaining = 0;
        lostExchanges.clear();
    }

    public synchronized int failuresRemaining()
    {
        return failuresRemaining;
    }

    /**
     * Exchange ids repeat across queries, so an exchange created with the id of a lost one starts with its data intact.
     */
    public synchronized void exchangeCreated(ExchangeId exchangeId)
    {
        lostExchanges.remove(exchangeId);
    }

    /**
     * While failures remain, the first read of an exchange loses its data and consumes a failure. Every later read of a
     * lost exchange fails as well, as it would with the data gone from storage, so the task retry granted on exchange
     * data loss fails too. Sibling readers of the same exchange may have reads in flight when the first one fails; they
     * do not consume failures, so the number of restarts does not depend on that race.
     */
    public synchronized boolean shouldFailRead(ExchangeId exchangeId)
    {
        if (lostExchanges.contains(exchangeId)) {
            return true;
        }
        if (failuresRemaining == 0) {
            return false;
        }
        failuresRemaining--;
        lostExchanges.add(exchangeId);
        return true;
    }
}
