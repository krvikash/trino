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
package io.starburst.stargate.tablemaintenance;

import static com.google.common.base.Preconditions.checkState;

public enum OptimizePositionDeletesMode
{
    INCLUDE_OPTIMIZE_POSITION_DELETES(1),
    EXCLUDE_OPTIMIZE_POSITION_DELETES(0);

    private final int numberOfQueries;

    OptimizePositionDeletesMode(int numberOfQueries)
    {
        checkState(numberOfQueries >= 0, "number of queries must be non-negative");
        this.numberOfQueries = numberOfQueries;
    }

    public int getNumberOfQueries()
    {
        return numberOfQueries;
    }
}
