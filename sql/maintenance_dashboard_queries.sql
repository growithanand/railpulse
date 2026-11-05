-- Read-only datasets for the RailPulse maintenance decision-support dashboard.
-- Execute each numbered statement separately in Databricks SQL.
-- The held-out metrics are frozen report-only evidence, not a live scoring query.

-- 1. Executive status and final held-out KPI tiles.
SELECT
    'railpulse-final-evaluation-v1' AS evidence_version,
    DATE '2026-11-04' AS evaluation_date,
    'NOT_PRODUCTION_READY' AS readiness_status,
    'motor-current-only' AS model_candidate_id,
    'p95-two-consecutive-cycles' AS alert_policy_candidate_id,
    4217 AS test_row_count,
    13 AS test_positive_count,
    4204 AS test_negative_count,
    CAST(0.0016708466249376135 AS DOUBLE) AS test_auprc,
    62 AS operating_day_count,
    1 AS accepted_failure_event_count,
    0 AS detected_failure_event_count,
    1 AS missed_failure_event_count,
    CAST(0.0 AS DOUBLE) AS event_detection_rate,
    73 AS false_alert_episode_count,
    CAST(1.1774193548387097 AS DOUBLE) AS false_alerts_per_operating_day,
    'Report only: do not retune or rerun July' AS result_use;

-- 2. Honest validation-versus-test comparison.
SELECT
    evaluation_order,
    evaluation_partition,
    row_count,
    positive_count,
    negative_count,
    auprc,
    operating_day_count,
    accepted_failure_event_count,
    detected_failure_event_count,
    missed_failure_event_count,
    event_detection_rate,
    warning_lead_time_seconds,
    false_alert_episode_count,
    false_alerts_per_operating_day
FROM VALUES
    (
        1, 'validation', 4730, 5, 4725, CAST(0.011954 AS DOUBLE), 28,
        1, 1, 0, CAST(1.0 AS DOUBLE), 1661, 33, CAST(1.1785714285714286 AS DOUBLE)
    ),
    (
        2, 'test', 4217, 13, 4204, CAST(0.0016708466249376135 AS DOUBLE), 62,
        1, 0, 1, CAST(0.0 AS DOUBLE), NULL, 73, CAST(1.1774193548387097 AS DOUBLE)
    ) AS evidence(
        evaluation_order,
        evaluation_partition,
        row_count,
        positive_count,
        negative_count,
        auprc,
        operating_day_count,
        accepted_failure_event_count,
        detected_failure_event_count,
        missed_failure_event_count,
        event_detection_rate,
        warning_lead_time_seconds,
        false_alert_episode_count,
        false_alerts_per_operating_day
    )
ORDER BY evaluation_order;

-- 3. Managed data-product row counts. This query does not read model scores.
WITH product_counts AS (
    SELECT 1 AS display_order, 'Silver' AS layer, 'Accepted telemetry' AS data_product,
        COUNT(*) AS row_count
    FROM workspace.railpulse_silver.telemetry_accepted
    UNION ALL
    SELECT 2, 'Silver', 'Quarantined telemetry', COUNT(*)
    FROM workspace.railpulse_silver.telemetry_quarantine
    UNION ALL
    SELECT 3, 'Silver', 'Accepted failure events', COUNT(*)
    FROM workspace.railpulse_silver.failure_events_accepted
    UNION ALL
    SELECT 4, 'Silver', 'Quarantined failure events', COUNT(*)
    FROM workspace.railpulse_silver.failure_events_quarantine
    UNION ALL
    SELECT 5, 'Gold', 'Loaded cycles', COUNT(*)
    FROM workspace.railpulse_gold.loaded_cycles
    UNION ALL
    SELECT 6, 'Gold', 'Feature snapshots v2', COUNT(*)
    FROM workspace.railpulse_gold.feature_snapshots_v2
    UNION ALL
    SELECT 7, 'Gold', 'Failure horizons', COUNT(*)
    FROM workspace.railpulse_gold.failure_horizons
    UNION ALL
    SELECT 8, 'Gold', 'Modelling view v2', COUNT(*)
    FROM workspace.railpulse_gold.modeling_view_v2
)
SELECT layer, data_product, row_count
FROM product_counts
ORDER BY display_order;

-- 4. Silver telemetry quality summary.
SELECT
    validation_version,
    total_record_count,
    accepted_record_count,
    quarantined_record_count,
    forward_gap_count,
    rejection_reason_counts
FROM workspace.railpulse_silver.telemetry_quality_metrics;

-- 5. Daily compressor-cycle volume and duration trend with no labels or model scores.
SELECT
    CAST(loaded_cycle_start_timestamp AS DATE) AS operating_date,
    COUNT(*) AS loaded_cycle_count,
    SUM(loaded_observation_count) AS loaded_observation_count,
    COUNT_IF(is_right_censored) AS right_censored_cycle_count,
    AVG(observed_duration_seconds) AS mean_observed_duration_seconds,
    PERCENTILE_APPROX(observed_duration_seconds, 0.5, 10000)
        AS median_observed_duration_seconds,
    PERCENTILE_APPROX(observed_duration_seconds, 0.95, 10000)
        AS p95_observed_duration_seconds
FROM workspace.railpulse_gold.loaded_cycles
GROUP BY CAST(loaded_cycle_start_timestamp AS DATE)
ORDER BY operating_date;

-- 6. Monthly accepted-failure summary without record IDs or report text.
SELECT
    CAST(DATE_TRUNC('MONTH', failure_start) AS DATE) AS failure_month,
    COUNT(*) AS accepted_failure_event_count,
    SUM(TIMESTAMPDIFF(MINUTE, failure_start, failure_end)) AS failure_duration_minutes
FROM workspace.railpulse_silver.failure_events_accepted
GROUP BY CAST(DATE_TRUNC('MONTH', failure_start) AS DATE)
ORDER BY failure_month;
