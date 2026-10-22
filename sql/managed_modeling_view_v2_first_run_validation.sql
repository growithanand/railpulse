-- Independent read-only checks for the first managed modelling-view v2 run.
-- Execute each statement separately against the development catalog.

-- 1. Population, versions, lineage, labels, and trainable prediction range.
SELECT
    COUNT(*) AS total_row_count,
    COUNT(loaded_cycle_id) AS non_null_key_count,
    COUNT(DISTINCT loaded_cycle_id) AS unique_key_count,
    COUNT_IF(modeling_row_status = 'trainable') AS trainable_count,
    COUNT_IF(modeling_row_status = 'excluded') AS excluded_count,
    COUNT_IF(
        modeling_row_status = 'trainable'
        AND failure_horizon_status = 'positive'
    ) AS trainable_positive_count,
    COUNT_IF(
        modeling_row_status = 'trainable'
        AND failure_horizon_status = 'negative'
    ) AS trainable_negative_count,
    COUNT(DISTINCT modeling_view_version) AS modeling_view_version_count,
    COUNT(DISTINCT feature_snapshot_version) AS snapshot_version_count,
    COUNT(DISTINCT motor_current_15m_feature_version) AS feature_version_count,
    COUNT(DISTINCT motor_current_eligibility_version) AS eligibility_version_count,
    COUNT(DISTINCT cycle_context_feature_version) AS context_version_count,
    COUNT(DISTINCT failure_horizon_version) AS horizon_version_count,
    COUNT(DISTINCT failure_horizon_seconds) AS horizon_seconds_count,
    COUNT(DISTINCT NAMED_STRUCT(
        'dataset_version', dataset_version,
        'telemetry_source_sha256', telemetry_source_sha256,
        'telemetry_ingestion_batch_id', telemetry_ingestion_batch_id,
        'failure_dataset_version', failure_dataset_version,
        'failure_source_sha256', failure_source_sha256,
        'failure_ingestion_batch_id', failure_ingestion_batch_id,
        'label_observation_end', label_observation_end
    )) AS source_identity_count,
    MIN(CASE WHEN modeling_row_status = 'trainable' THEN prediction_timestamp END)
        AS earliest_trainable_prediction,
    MAX(CASE WHEN modeling_row_status = 'trainable' THEN prediction_timestamp END)
        AS latest_trainable_prediction
FROM workspace.railpulse_gold.modeling_view_v2;

-- 2. Full source-projection reconciliation. Every mismatch count must be zero.
WITH model_features AS (
    SELECT
        loaded_cycle_id,
        motor_current_15m_feature_version,
        motor_current_15m_window_seconds,
        motor_current_15m_window_start,
        motor_current_15m_status,
        motor_current_15m_observation_count,
        motor_current_15m_first_observation_timestamp,
        motor_current_15m_last_observation_timestamp,
        motor_current_15m_minimum_amperes,
        motor_current_15m_mean_amperes,
        motor_current_15m_maximum_amperes,
        motor_current_eligibility_version,
        motor_current_eligibility_minimum_excluded_gap_seconds,
        motor_current_maximum_window_gap_seconds,
        motor_current_eligibility_status,
        motor_current_eligibility_reasons,
        cycle_context_feature_version,
        cycle_context_status,
        cycle_context_current_duration_seconds,
        cycle_context_previous_cycle_id,
        cycle_context_previous_duration_seconds,
        cycle_context_previous_idle_seconds,
        feature_snapshot_version,
        dataset_version,
        telemetry_source_sha256,
        telemetry_ingestion_batch_id
    FROM workspace.railpulse_gold.modeling_view_v2
),
source_features AS (
    SELECT * FROM workspace.railpulse_gold.feature_snapshots_v2
),
model_horizons AS (
    SELECT
        loaded_cycle_id,
        failure_horizon_version,
        failure_horizon_seconds,
        label_observation_end,
        prediction_timestamp,
        failure_horizon_end,
        failure_horizon_status,
        failure_within_horizon,
        matched_failure_record_id,
        matched_failure_source_row,
        matched_failure_start,
        matched_failure_end,
        seconds_to_failure
    FROM workspace.railpulse_gold.modeling_view_v2
),
source_horizons AS (
    SELECT
        loaded_cycle_id,
        failure_horizon_version,
        failure_horizon_seconds,
        label_observation_end,
        prediction_timestamp,
        failure_horizon_end,
        failure_horizon_status,
        failure_within_horizon,
        matched_failure_record_id,
        matched_failure_source_row,
        matched_failure_start,
        matched_failure_end,
        seconds_to_failure
    FROM workspace.railpulse_gold.failure_horizons
),
missing_model_features AS (
    SELECT * FROM source_features EXCEPT SELECT * FROM model_features
),
unexpected_model_features AS (
    SELECT * FROM model_features EXCEPT SELECT * FROM source_features
),
missing_model_horizons AS (
    SELECT * FROM source_horizons EXCEPT SELECT * FROM model_horizons
),
unexpected_model_horizons AS (
    SELECT * FROM model_horizons EXCEPT SELECT * FROM source_horizons
)
SELECT
    (SELECT COUNT(*) FROM source_features) AS feature_source_count,
    (SELECT COUNT(*) FROM model_features) AS model_feature_count,
    (SELECT COUNT(*) FROM missing_model_features) AS missing_feature_row_count,
    (SELECT COUNT(*) FROM unexpected_model_features) AS unexpected_feature_row_count,
    (SELECT COUNT(*) FROM source_horizons) AS horizon_source_count,
    (SELECT COUNT(*) FROM model_horizons) AS model_horizon_count,
    (SELECT COUNT(*) FROM missing_model_horizons) AS missing_horizon_row_count,
    (SELECT COUNT(*) FROM unexpected_model_horizons) AS unexpected_horizon_row_count;

-- 3. Auditable distributions for row status, horizon status, labels, and exclusion reasons.
SELECT
    'modeling_row_status' AS metric_group,
    modeling_row_status AS metric_name,
    COUNT(*) AS row_count
FROM workspace.railpulse_gold.modeling_view_v2
GROUP BY modeling_row_status
UNION ALL
SELECT 'failure_horizon_status', failure_horizon_status, COUNT(*)
FROM workspace.railpulse_gold.modeling_view_v2
GROUP BY failure_horizon_status
UNION ALL
SELECT 'trainable_label', failure_horizon_status, COUNT(*)
FROM workspace.railpulse_gold.modeling_view_v2
WHERE modeling_row_status = 'trainable'
GROUP BY failure_horizon_status
UNION ALL
SELECT 'exclusion_reason', reason, COUNT(*)
FROM workspace.railpulse_gold.modeling_view_v2
LATERAL VIEW EXPLODE(modeling_exclusion_reasons) reasons AS reason
GROUP BY reason
ORDER BY metric_group, metric_name;

-- 4. Contract, source, target, row-state, and leakage-boundary checks.
-- Every result must be zero.
SELECT
    COUNT_IF(loaded_cycle_id IS NULL OR LENGTH(loaded_cycle_id) = 0)
        AS invalid_key_count,
    COUNT_IF(NOT (
        modeling_view_version <=> 'motor-current-cycle-context-failure-modeling-view-v2'
        AND feature_snapshot_version <=> 'motor-current-15m-cycle-context-snapshot-v2'
        AND motor_current_15m_feature_version <=> 'motor-current-15m-v1'
        AND motor_current_eligibility_version <=> 'motor-current-15m-eligibility-v1'
        AND cycle_context_feature_version <=> 'loaded-cycle-operating-context-v1'
        AND failure_horizon_version <=> 'cycle-failure-horizon-v1'
        AND failure_horizon_seconds <=> 7200
    )) AS fixed_contract_violation_count,
    COUNT_IF(
        dataset_version IS NULL OR TRIM(dataset_version) = ''
        OR telemetry_source_sha256 IS NULL OR TRIM(telemetry_source_sha256) = ''
        OR telemetry_ingestion_batch_id IS NULL OR TRIM(telemetry_ingestion_batch_id) = ''
        OR failure_dataset_version IS NULL OR TRIM(failure_dataset_version) = ''
        OR failure_source_sha256 IS NULL OR TRIM(failure_source_sha256) = ''
        OR failure_ingestion_batch_id IS NULL OR TRIM(failure_ingestion_batch_id) = ''
        OR label_observation_end IS NULL
        OR NOT (dataset_version <=> failure_dataset_version)
    ) AS lineage_violation_count,
    COUNT_IF(
        motor_current_eligibility_status NOT IN ('eligible', 'ineligible')
        OR motor_current_eligibility_status IS NULL
        OR cycle_context_status NOT IN (
            'available',
            'missing_prediction_boundary',
            'left_censored_current_cycle',
            'incomplete_current_cycle',
            'missing_previous_cycle',
            'incomplete_previous_cycle',
            'invalid_previous_interval'
        )
        OR cycle_context_status IS NULL
        OR failure_horizon_status NOT IN (
            'positive',
            'negative',
            'horizon_censored',
            'inside_failure_interval',
            'missing_prediction_boundary'
        )
        OR failure_horizon_status IS NULL
    ) AS unsupported_source_status_count,
    COUNT_IF(NOT (
        (failure_horizon_status = 'positive' AND (failure_within_horizon <=> TRUE))
        OR (failure_horizon_status = 'negative' AND (failure_within_horizon <=> FALSE))
        OR (
            failure_horizon_status IN (
                'horizon_censored',
                'inside_failure_interval',
                'missing_prediction_boundary'
            )
            AND failure_within_horizon IS NULL
        )
    )) AS target_semantics_violation_count,
    COUNT_IF(NOT (
        modeling_row_status <=> CASE
            WHEN motor_current_eligibility_status = 'eligible'
                AND cycle_context_status = 'available'
                AND failure_horizon_status IN ('positive', 'negative')
            THEN 'trainable'
            ELSE 'excluded'
        END
    )) AS row_status_violation_count,
    COUNT_IF(NOT (
        modeling_exclusion_reasons <=> ARRAY_COMPACT(ARRAY(
            CASE WHEN motor_current_eligibility_status = 'ineligible'
                THEN 'feature_ineligible' END,
            CASE WHEN cycle_context_status <> 'available'
                THEN 'cycle_context_unavailable' END,
            CASE WHEN failure_horizon_status = 'horizon_censored'
                THEN 'horizon_censored' END,
            CASE WHEN failure_horizon_status = 'inside_failure_interval'
                THEN 'inside_failure_interval' END,
            CASE WHEN failure_horizon_status = 'missing_prediction_boundary'
                THEN 'missing_prediction_boundary' END
        ))
    )) AS exclusion_reason_violation_count,
    COUNT_IF(NOT (
        motor_current_15m_window_start
        <=> TIMESTAMPADD(
            SECOND,
            -motor_current_15m_window_seconds,
            prediction_timestamp
        )
    )) AS feature_window_boundary_violation_count,
    COUNT_IF(
        modeling_row_status = 'trainable'
        AND (
            prediction_timestamp IS NULL
            OR motor_current_15m_observation_count <= 0
            OR motor_current_15m_minimum_amperes IS NULL
            OR motor_current_15m_mean_amperes IS NULL
            OR motor_current_15m_maximum_amperes IS NULL
            OR motor_current_maximum_window_gap_seconds IS NULL
            OR cycle_context_current_duration_seconds IS NULL
            OR cycle_context_previous_duration_seconds IS NULL
            OR cycle_context_previous_idle_seconds IS NULL
            OR failure_within_horizon IS NULL
            OR SIZE(modeling_exclusion_reasons) <> 0
        )
    ) AS trainable_input_violation_count,
    COUNT_IF(
        modeling_row_status = 'excluded'
        AND SIZE(modeling_exclusion_reasons) = 0
    ) AS excluded_reason_violation_count
FROM workspace.railpulse_gold.modeling_view_v2;

-- 5. Failure lineage and positive references must resolve to accepted Silver failures.
WITH model_lineage AS (
    SELECT DISTINCT
        failure_dataset_version,
        failure_source_sha256,
        failure_ingestion_batch_id
    FROM workspace.railpulse_gold.modeling_view_v2
),
accepted_lineage AS (
    SELECT DISTINCT
        dataset_version,
        source_sha256,
        ingestion_batch_id
    FROM workspace.railpulse_silver.failure_events_accepted
),
unmatched_lineage AS (
    SELECT * FROM model_lineage
    LEFT ANTI JOIN accepted_lineage
        ON model_lineage.failure_dataset_version = accepted_lineage.dataset_version
        AND model_lineage.failure_source_sha256 = accepted_lineage.source_sha256
        AND model_lineage.failure_ingestion_batch_id = accepted_lineage.ingestion_batch_id
),
positive_reference_checks AS (
    SELECT
        COUNT_IF(
            model.modeling_row_status = 'trainable'
            AND model.failure_horizon_status = 'positive'
            AND failures.record_id IS NULL
        ) AS unresolved_trainable_positive_count,
        COUNT_IF(
            NOT (
                model.modeling_row_status = 'trainable'
                AND model.failure_horizon_status = 'positive'
            )
            AND failures.record_id IS NOT NULL
        ) AS unexpected_failure_reference_count
    FROM workspace.railpulse_gold.modeling_view_v2 AS model
    LEFT JOIN workspace.railpulse_silver.failure_events_accepted AS failures
        ON failures.record_id = model.matched_failure_record_id
        AND failures.source_row = model.matched_failure_source_row
        AND failures.failure_start = model.matched_failure_start
        AND failures.failure_end = model.matched_failure_end
)
SELECT
    (SELECT COUNT(*) FROM unmatched_lineage) AS failure_lineage_mismatch_count,
    unresolved_trainable_positive_count,
    unexpected_failure_reference_count
FROM positive_reference_checks;

-- 6. Trainable positive-cycle coverage for each accepted failure event.
WITH represented_failures AS (
    SELECT
        matched_failure_record_id,
        COUNT(*) AS trainable_positive_cycle_count
    FROM workspace.railpulse_gold.modeling_view_v2
    WHERE modeling_row_status = 'trainable'
        AND failure_horizon_status = 'positive'
    GROUP BY matched_failure_record_id
),
event_coverage AS (
    SELECT
        failures.record_id,
        failures.source_row,
        failures.failure_start,
        failures.failure_end,
        COALESCE(represented.trainable_positive_cycle_count, 0)
            AS trainable_positive_cycle_count
    FROM workspace.railpulse_silver.failure_events_accepted AS failures
    LEFT JOIN represented_failures AS represented
        ON represented.matched_failure_record_id = failures.record_id
)
SELECT
    source_row,
    failure_start,
    failure_end,
    trainable_positive_cycle_count,
    COUNT(*) OVER () AS accepted_failure_event_count,
    COUNT_IF(trainable_positive_cycle_count > 0) OVER () AS represented_failure_event_count,
    SUM(trainable_positive_cycle_count) OVER () AS total_trainable_positive_cycle_count
FROM event_coverage
ORDER BY source_row;
