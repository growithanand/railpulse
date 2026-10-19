-- Independent read-only checks for the first managed failure-horizon run.
-- Execute each statement separately against the development catalog.

-- 1. Key, label-distribution, version, horizon, and observation-boundary reconciliation.
SELECT
    COUNT(*) AS total_rows,
    COUNT(loaded_cycle_id) AS non_null_ids,
    COUNT(DISTINCT loaded_cycle_id) AS unique_ids,
    COUNT_IF(failure_horizon_status = 'positive') AS positive_count,
    COUNT_IF(failure_horizon_status = 'negative') AS negative_count,
    COUNT_IF(failure_horizon_status = 'horizon_censored') AS horizon_censored_count,
    COUNT_IF(failure_horizon_status = 'inside_failure_interval')
        AS inside_failure_interval_count,
    COUNT_IF(failure_horizon_status = 'missing_prediction_boundary')
        AS missing_prediction_boundary_count,
    COUNT(DISTINCT failure_horizon_version) AS horizon_version_count,
    MIN(failure_horizon_version) AS horizon_version,
    COUNT(DISTINCT failure_horizon_seconds) AS horizon_seconds_count,
    MIN(failure_horizon_seconds) AS horizon_seconds,
    COUNT(DISTINCT label_observation_end) AS observation_end_count,
    MIN(label_observation_end) AS observation_end_min,
    MAX(label_observation_end) AS observation_end_max
FROM workspace.railpulse_gold.failure_horizons;

-- 2. Row-level contract and status-semantics checks. Every result must be zero.
SELECT
    COUNT_IF(loaded_cycle_id IS NULL OR LENGTH(loaded_cycle_id) = 0)
        AS invalid_key_count,
    COUNT_IF(
        NOT (failure_horizon_version <=> 'cycle-failure-horizon-v1')
        OR NOT (failure_horizon_seconds <=> 7200)
        OR label_observation_end IS NULL
    ) AS contract_violation_count,
    COUNT_IF(
        NOT (prediction_timestamp <=> loaded_cycle_stop_timestamp)
        OR NOT (
            failure_horizon_end
            <=> TIMESTAMPADD(SECOND, failure_horizon_seconds, prediction_timestamp)
        )
    ) AS boundary_violation_count,
    COUNT_IF(
        failure_horizon_status = 'positive'
        AND NOT COALESCE(
            (failure_within_horizon <=> TRUE)
            AND prediction_timestamp IS NOT NULL
            AND failure_horizon_end IS NOT NULL
            AND matched_failure_record_id IS NOT NULL
            AND matched_failure_source_row IS NOT NULL
            AND matched_failure_start IS NOT NULL
            AND matched_failure_end IS NOT NULL
            AND seconds_to_failure IS NOT NULL
            AND matched_failure_start > prediction_timestamp
            AND matched_failure_start <= failure_horizon_end
            AND matched_failure_start <= label_observation_end
            AND seconds_to_failure = TIMESTAMPDIFF(
                SECOND,
                prediction_timestamp,
                matched_failure_start
            )
            AND seconds_to_failure > 0
            AND seconds_to_failure <= failure_horizon_seconds,
            FALSE
        )
    ) AS positive_violation_count,
    COUNT_IF(
        failure_horizon_status = 'negative'
        AND NOT COALESCE(
            (failure_within_horizon <=> FALSE)
            AND prediction_timestamp IS NOT NULL
            AND failure_horizon_end IS NOT NULL
            AND failure_horizon_end <= label_observation_end
            AND matched_failure_record_id IS NULL
            AND matched_failure_source_row IS NULL
            AND matched_failure_start IS NULL
            AND matched_failure_end IS NULL
            AND seconds_to_failure IS NULL,
            FALSE
        )
    ) AS negative_violation_count,
    COUNT_IF(
        failure_horizon_status = 'horizon_censored'
        AND NOT COALESCE(
            failure_within_horizon IS NULL
            AND prediction_timestamp IS NOT NULL
            AND failure_horizon_end IS NOT NULL
            AND failure_horizon_end > label_observation_end
            AND matched_failure_record_id IS NULL
            AND matched_failure_source_row IS NULL
            AND matched_failure_start IS NULL
            AND matched_failure_end IS NULL
            AND seconds_to_failure IS NULL,
            FALSE
        )
    ) AS censored_violation_count,
    COUNT_IF(
        failure_horizon_status = 'inside_failure_interval'
        AND NOT COALESCE(
            failure_within_horizon IS NULL
            AND prediction_timestamp IS NOT NULL
            AND failure_horizon_end IS NOT NULL
            AND matched_failure_record_id IS NULL
            AND matched_failure_source_row IS NULL
            AND matched_failure_start IS NULL
            AND matched_failure_end IS NULL
            AND seconds_to_failure IS NULL,
            FALSE
        )
    ) AS inside_status_violation_count,
    COUNT_IF(
        failure_horizon_status = 'missing_prediction_boundary'
        AND NOT COALESCE(
            failure_within_horizon IS NULL
            AND prediction_timestamp IS NULL
            AND loaded_cycle_stop_timestamp IS NULL
            AND failure_horizon_end IS NULL
            AND matched_failure_record_id IS NULL
            AND matched_failure_source_row IS NULL
            AND matched_failure_start IS NULL
            AND matched_failure_end IS NULL
            AND seconds_to_failure IS NULL,
            FALSE
        )
    ) AS missing_boundary_violation_count,
    COUNT_IF(
        failure_horizon_status NOT IN (
            'positive',
            'negative',
            'horizon_censored',
            'inside_failure_interval',
            'missing_prediction_boundary'
        )
        OR failure_horizon_status IS NULL
    ) AS unsupported_status_count
FROM workspace.railpulse_gold.failure_horizons;

-- 3. Positive labels must reference an exact accepted Silver failure row.
WITH horizons AS (
    SELECT * FROM workspace.railpulse_gold.failure_horizons
),
failures AS (
    SELECT * FROM workspace.railpulse_silver.failure_events_accepted
)
SELECT
    COUNT_IF(
        horizons.failure_horizon_status = 'positive'
        AND failures.record_id IS NULL
    ) AS positive_reference_violation_count,
    COUNT_IF(
        horizons.failure_horizon_status <> 'positive'
        AND failures.record_id IS NOT NULL
    ) AS unexpected_matched_reference_count
FROM horizons
LEFT JOIN failures
    ON failures.record_id = horizons.matched_failure_record_id
    AND failures.source_row = horizons.matched_failure_source_row
    AND failures.failure_start = horizons.matched_failure_start
    AND failures.failure_end = horizons.matched_failure_end;

-- 4. In-failure exclusions must exactly match published failure intervals.
WITH interval_matches AS (
    SELECT
        horizons.loaded_cycle_id,
        horizons.failure_horizon_status,
        MAX(CASE WHEN failures.record_id IS NOT NULL THEN 1 ELSE 0 END)
            AS inside_published_interval
    FROM workspace.railpulse_gold.failure_horizons AS horizons
    LEFT JOIN workspace.railpulse_silver.failure_events_accepted AS failures
        ON failures.failure_start <= horizons.prediction_timestamp
        AND failures.failure_end >= horizons.prediction_timestamp
    GROUP BY horizons.loaded_cycle_id, horizons.failure_horizon_status
)
SELECT
    COUNT_IF(
        failure_horizon_status = 'inside_failure_interval'
        AND inside_published_interval = 0
    ) AS missing_inside_interval_reference_count,
    COUNT_IF(
        failure_horizon_status <> 'inside_failure_interval'
        AND inside_published_interval = 1
    ) AS unmarked_inside_interval_count
FROM interval_matches;
