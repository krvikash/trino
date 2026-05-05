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
package io.trino.operator.gpu;

import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import static java.util.Objects.requireNonNull;

public class SentinelSinkOperator
        implements Operator
{
    public static class Factory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        public Factory(int operatorId, PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            return new SentinelSinkOperator(driverContext.addOperatorContext(operatorId, planNodeId, SentinelSinkOperator.class.getSimpleName()));
        }

        @Override
        public void noMoreOperators() {}

        @Override
        public OperatorFactory duplicate()
        {
            return new Factory(operatorId, planNodeId);
        }
    }

    private final OperatorContext operatorContext;

    private boolean finished;

    public SentinelSinkOperator(OperatorContext operatorContext)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException("Any input is unexpected");
    }

    @Override
    public Page getOutput()
    {
        throw new UnsupportedOperationException("This operator should be last in the pipeline and getOutput should never be called");
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }
}
