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
package io.trino.spi.eventlistener;

/**
 * Outcome of evaluating a single materialized view candidate against a base table scan during
 * materialized-view substitution.
 */
public enum MaterializedViewSubstitutionOutcome
{
    /**
     * The scan was rewritten to read the materialized view's storage table.
     */
    SUBSTITUTED,
    /**
     * The scan is not eligible for substitution at all: it is an update target, has an enforced
     * constraint, or has connector node partitioning. Not tied to any particular candidate.
     */
    SCAN_NOT_SUBSTITUTABLE,
    /**
     * The materialization is outside its grace period.
     */
    STALE,
    /**
     * The materialization exceeds the session's {@code materialized_view_substitution_max_staleness}.
     */
    MAX_STALENESS_EXCEEDED,
    /**
     * The materialized view's name does not match {@code materialized_view_substitution_candidates_regex_filter}.
     */
    EXCLUDED_BY_FILTER,
    /**
     * The query computation does not structurally match the materialization, the source table differs,
     * or a queried column has no identifiable id.
     */
    COMPUTATION_MISMATCH,
    /**
     * A queried column is not exposed by the materialized view's computation.
     */
    COLUMN_NOT_IN_MATERIALIZED_VIEW,
    /**
     * A queried column's type does not match the type recorded for it in the materialized view's computation.
     */
    COLUMN_TYPE_MISMATCH,
    /**
     * The materialized view's storage table no longer resolves to a table handle.
     */
    STORAGE_TABLE_NOT_FOUND,
    /**
     * The storage table resolved by name today does not report the same id captured when the
     * materialization was indexed: it advanced via a divergent refresh, or was dropped and recreated.
     */
    STORAGE_TABLE_CHANGED,
    /**
     * The storage table no longer exposes a column the materialized view's computation requires.
     */
    STORAGE_COLUMN_MISSING,
    /**
     * The storage column's type cannot be coerced to the query column's type.
     */
    STORAGE_TYPE_NOT_COERCIBLE,
    /**
     * The user does not have SELECT access to the materialized view itself.
     */
    ACCESS_DENIED,
    /**
     * The user has SELECT access to the materialized view but not to one of the columns the
     * substitution would read from it.
     */
    COLUMN_ACCESS_DENIED,
}
