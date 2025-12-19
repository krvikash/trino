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
package com.starburstdata.plugin.openapi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_INVALID_FILTER;
import static java.util.Objects.requireNonNull;

public class OpenApiTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final List<String> selectPaths;
    private final PathItem.HttpMethod selectMethod;
    private final List<String> insertPaths;
    private final PathItem.HttpMethod insertMethod;
    private final List<String> updatePaths;
    private final PathItem.HttpMethod updateMethod;
    private final List<String> deletePaths;
    private final PathItem.HttpMethod deleteMethod;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public OpenApiTableHandle(
            SchemaTableName schemaTableName,
            List<String> selectPaths,
            PathItem.HttpMethod selectMethod,
            List<String> insertPaths,
            PathItem.HttpMethod insertMethod,
            List<String> updatePaths,
            PathItem.HttpMethod updateMethod,
            List<String> deletePaths,
            PathItem.HttpMethod deleteMethod,
            TupleDomain<ColumnHandle> constraint)
    {
        this.schemaTableName = schemaTableName;
        this.selectPaths = ImmutableList.copyOf(selectPaths);
        this.selectMethod = requireNonNull(selectMethod, "selectMethod is null");
        this.insertPaths = ImmutableList.copyOf(insertPaths);
        this.insertMethod = requireNonNull(insertMethod, "insertMethod is null");
        this.updatePaths = ImmutableList.copyOf(updatePaths);
        this.updateMethod = requireNonNull(updateMethod, "updateMethod is null");
        this.deletePaths = ImmutableList.copyOf(deletePaths);
        this.deleteMethod = requireNonNull(deleteMethod, "deleteMethod is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<String> getSelectPaths()
    {
        return selectPaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getSelectMethod()
    {
        return selectMethod;
    }

    @JsonProperty
    public List<String> getInsertPaths()
    {
        return insertPaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getInsertMethod()
    {
        return insertMethod;
    }

    @JsonProperty
    public List<String> getUpdatePaths()
    {
        return updatePaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getUpdateMethod()
    {
        return updateMethod;
    }

    @JsonProperty
    public List<String> getDeletePaths()
    {
        return deletePaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getDeleteMethod()
    {
        return deleteMethod;
    }

    @JsonProperty("constraint")
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public String toString()
    {
        return schemaTableName.getTableName();
    }

    public OpenApiTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new OpenApiTableHandle(
                schemaTableName,
                selectPaths,
                selectMethod,
                insertPaths,
                insertMethod,
                updatePaths,
                updateMethod,
                deletePaths,
                deleteMethod,
                constraint);
    }

    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(Constraint constraint, Map<String, OpenApiColumn> columns, int domainExpansionLimit)
    {
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        // the only reason not to use isNone is so the linter doesn't complain about not checking an Optional
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = getConstraint();

        boolean found = false;
        for (OpenApiColumn column : columns.values()) {
            if (column.getRequiresPredicate().isEmpty() && column.getOptionalPredicate().isEmpty()) {
                continue;
            }

            TupleDomain<ColumnHandle> newConstraint = normalizeConstraint(column.getHandle(), summary, domainExpansionLimit);
            if (newConstraint == null || newConstraint.getDomains().isEmpty()) {
                continue;
            }
            if (!validateConstraint(column.getHandle(), currentConstraint, newConstraint)) {
                continue;
            }
            // merge with other pushed down constraints
            Domain domain = newConstraint.getDomains().get().get(column.getHandle());
            if (currentConstraint.getDomains().isEmpty()) {
                currentConstraint = newConstraint;
            }
            else if (!currentConstraint.getDomains().get().containsKey(column.getHandle())) {
                Map<ColumnHandle, Domain> domains = new HashMap<>(currentConstraint.getDomains().get());
                domains.put(column.getHandle(), domain);
                currentConstraint = TupleDomain.withColumnDomains(domains);
            }
            else {
                currentConstraint.getDomains().get().get(column.getHandle()).union(domain);
            }
            found = true;
            // remove from remaining constraints
            summary = summary.filter(
                    (columnHandle, tupleDomain) -> !columnHandle.equals(column.getHandle()));
        }
        if (!found) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                withConstraint(currentConstraint),
                summary,
                constraint.getExpression(),
                true));
    }

    private TupleDomain<ColumnHandle> normalizeConstraint(OpenApiColumnHandle column, TupleDomain<ColumnHandle> constraint, int domainExpansionLimit)
    {
        //noinspection OptionalGetWithoutIsPresent
        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return null;
        }
        TupleDomain<ColumnHandle> newConstraint = constraint.filter(
                (columnHandle, tupleDomain) -> columnHandle.equals(column));
        if (domain.getValues().isDiscreteSet()) {
            return newConstraint;
        }
        return domain.getValues().tryExpandRanges(domainExpansionLimit)
                .map(ranges -> TupleDomain.withColumnDomains(Map.of(
                        (ColumnHandle) column,
                        Domain.multipleValues(domain.getType(), ranges.stream().collect(toImmutableList())))))
                .orElse(null);
    }

    private boolean validateConstraint(OpenApiColumnHandle column, TupleDomain<ColumnHandle> currentConstraint, TupleDomain<ColumnHandle> newConstraint)
    {
        if (currentConstraint.getDomains().isEmpty() || !currentConstraint.getDomains().get().containsKey(column)) {
            return true;
        }
        Domain currentDomain = currentConstraint.getDomains().get().get(column);
        Domain newDomain = newConstraint.getDomains().get().get(column);
        if (currentDomain.equals(newDomain)) {
            // it is important to avoid processing same constraint multiple times
            // so that planner doesn't get stuck in a loop
            return false;
        }
        // can push down only the first predicate against this column
        throw new TrinoException(OPENAPI_INVALID_FILTER, "Already pushed down a predicate for " + column.name() + " which only supports a single value");
    }
}
