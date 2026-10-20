-- Read-only validation for the first managed Gold feature-snapshot v2 run.
-- Run each statement independently against the development Unity Catalog namespace.

-- 1. Population, key, version, lineage, feature, eligibility, and context summary.
SELECT
  COUNT(*) AS total_row_count,
  COUNT(loaded_cycle_id) AS non_null_key_count,
  COUNT(DISTINCT loaded_cycle_id) AS unique_key_count,
  COUNT(DISTINCT feature_snapshot_version) AS snapshot_version_count,
  COUNT(DISTINCT motor_current_15m_feature_version) AS feature_version_count,
  COUNT(DISTINCT motor_current_eligibility_version) AS eligibility_version_count,
  COUNT(DISTINCT cycle_context_feature_version) AS context_version_count,
  COUNT(DISTINCT motor_current_15m_window_seconds) AS window_seconds_count,
  COUNT(DISTINCT motor_current_eligibility_minimum_excluded_gap_seconds)
    AS gap_threshold_count,
  COUNT(DISTINCT named_struct(
    'dataset_version', dataset_version,
    'source_sha256', telemetry_source_sha256,
    'ingestion_batch_id', telemetry_ingestion_batch_id
  )) AS source_lineage_count,
  COUNT_IF(motor_current_15m_status = 'available') AS available_feature_count,
  COUNT_IF(motor_current_15m_status = 'missing_prediction_boundary')
    AS missing_prediction_feature_count,
  COUNT_IF(motor_current_eligibility_status = 'eligible') AS eligible_count,
  COUNT_IF(motor_current_eligibility_status = 'ineligible') AS ineligible_count,
  COUNT_IF(cycle_context_status = 'available') AS available_context_count,
  COUNT_IF(cycle_context_status = 'missing_prediction_boundary')
    AS missing_prediction_context_count,
  COUNT_IF(cycle_context_status = 'left_censored_current_cycle')
    AS left_censored_context_count,
  COUNT_IF(cycle_context_status = 'missing_previous_cycle') AS missing_previous_context_count,
  COUNT_IF(cycle_context_status = 'incomplete_previous_cycle')
    AS incomplete_previous_context_count
FROM workspace.railpulse_gold.feature_snapshots_v2;

-- 2. Exact key reconciliation against the governed loaded-cycle source.
WITH missing_feature_keys AS (
  SELECT loaded_cycle_id FROM workspace.railpulse_gold.loaded_cycles
  EXCEPT
  SELECT loaded_cycle_id FROM workspace.railpulse_gold.feature_snapshots_v2
),
unexpected_feature_keys AS (
  SELECT loaded_cycle_id FROM workspace.railpulse_gold.feature_snapshots_v2
  EXCEPT
  SELECT loaded_cycle_id FROM workspace.railpulse_gold.loaded_cycles
)
SELECT
  (SELECT COUNT(*) FROM workspace.railpulse_gold.loaded_cycles) AS loaded_cycle_count,
  (SELECT COUNT(*) FROM workspace.railpulse_gold.feature_snapshots_v2) AS snapshot_count,
  (SELECT COUNT(*) FROM missing_feature_keys) AS missing_feature_key_count,
  (SELECT COUNT(*) FROM unexpected_feature_keys) AS unexpected_feature_key_count;

-- 3. Auditable distributions for every explicit status and eligibility reason.
SELECT 'feature_status' AS metric_group, motor_current_15m_status AS metric_name, COUNT(*) AS row_count
FROM workspace.railpulse_gold.feature_snapshots_v2
GROUP BY motor_current_15m_status
UNION ALL
SELECT 'eligibility_status', motor_current_eligibility_status, COUNT(*)
FROM workspace.railpulse_gold.feature_snapshots_v2
GROUP BY motor_current_eligibility_status
UNION ALL
SELECT 'cycle_context_status', cycle_context_status, COUNT(*)
FROM workspace.railpulse_gold.feature_snapshots_v2
GROUP BY cycle_context_status
UNION ALL
SELECT 'eligibility_reason', reason, COUNT(*)
FROM workspace.railpulse_gold.feature_snapshots_v2
LATERAL VIEW explode(motor_current_eligibility_reasons) reasons AS reason
GROUP BY reason
ORDER BY metric_group, metric_name;

-- 4. Semantic, source-boundary, and label-isolation violations. Every value must be zero.
WITH snapshot_violations AS (
  SELECT
    COUNT_IF(
      feature_snapshot_version <> 'motor-current-15m-cycle-context-snapshot-v2'
      OR motor_current_15m_feature_version <> 'motor-current-15m-v1'
      OR motor_current_eligibility_version <> 'motor-current-15m-eligibility-v1'
      OR cycle_context_feature_version <> 'loaded-cycle-operating-context-v1'
      OR motor_current_15m_window_seconds <> 900
      OR motor_current_eligibility_minimum_excluded_gap_seconds <> 20
    ) AS invalid_fixed_contract_count,
    COUNT_IF(
      dataset_version IS NULL OR trim(dataset_version) = ''
      OR telemetry_source_sha256 IS NULL OR trim(telemetry_source_sha256) = ''
      OR telemetry_ingestion_batch_id IS NULL OR trim(telemetry_ingestion_batch_id) = ''
    ) AS incomplete_lineage_count,
    COUNT_IF(NOT (
      (
        motor_current_15m_status = 'available'
        AND motor_current_15m_observation_count > 0
        AND motor_current_15m_window_start IS NOT NULL
        AND motor_current_15m_first_observation_timestamp IS NOT NULL
        AND motor_current_15m_last_observation_timestamp IS NOT NULL
        AND motor_current_15m_minimum_amperes IS NOT NULL
        AND motor_current_15m_mean_amperes IS NOT NULL
        AND motor_current_15m_maximum_amperes IS NOT NULL
      )
      OR (
        motor_current_15m_status = 'missing_prediction_boundary'
        AND motor_current_15m_observation_count = 0
        AND motor_current_15m_window_start IS NULL
        AND motor_current_15m_first_observation_timestamp IS NULL
        AND motor_current_15m_last_observation_timestamp IS NULL
        AND motor_current_15m_minimum_amperes IS NULL
        AND motor_current_15m_mean_amperes IS NULL
        AND motor_current_15m_maximum_amperes IS NULL
      )
    )) AS invalid_feature_semantics_count,
    COUNT_IF(NOT (
      (
        motor_current_eligibility_status = 'eligible'
        AND motor_current_15m_status = 'available'
        AND motor_current_maximum_window_gap_seconds < 20
        AND size(motor_current_eligibility_reasons) = 0
      )
      OR (
        motor_current_eligibility_status = 'ineligible'
        AND motor_current_15m_status = 'available'
        AND motor_current_maximum_window_gap_seconds >= 20
        AND motor_current_eligibility_reasons = array('material_window_gap')
      )
      OR (
        motor_current_eligibility_status = 'ineligible'
        AND motor_current_15m_status = 'missing_prediction_boundary'
        AND motor_current_maximum_window_gap_seconds IS NULL
        AND motor_current_eligibility_reasons = array('feature_missing_prediction_boundary')
      )
    )) AS invalid_eligibility_semantics_count,
    COUNT_IF(
      cycle_context_status NOT IN (
        'available',
        'missing_prediction_boundary',
        'left_censored_current_cycle',
        'incomplete_current_cycle',
        'missing_previous_cycle',
        'incomplete_previous_cycle',
        'invalid_previous_interval'
      )
      OR cycle_context_current_duration_seconds <= 0
      OR cycle_context_previous_duration_seconds <= 0
      OR cycle_context_previous_idle_seconds < 0
      OR (
        cycle_context_status = 'available'
        AND (
          cycle_context_current_duration_seconds IS NULL
          OR cycle_context_previous_cycle_id IS NULL
          OR cycle_context_previous_duration_seconds IS NULL
          OR cycle_context_previous_idle_seconds IS NULL
        )
      )
    ) AS invalid_context_semantics_count
  FROM workspace.railpulse_gold.feature_snapshots_v2
),
boundary_violations AS (
  SELECT COUNT_IF(NOT (
    (
      snapshots.motor_current_15m_status = 'available'
      AND snapshots.motor_current_15m_last_observation_timestamp
        <=> cycles.loaded_cycle_stop_timestamp
      AND snapshots.motor_current_15m_window_start
        <=> timestampadd(SECOND, -900, cycles.loaded_cycle_stop_timestamp)
    )
    OR (
      snapshots.motor_current_15m_status = 'missing_prediction_boundary'
      AND cycles.loaded_cycle_stop_timestamp IS NULL
    )
  )) AS cycle_boundary_mismatch_count
  FROM workspace.railpulse_gold.feature_snapshots_v2 AS snapshots
  INNER JOIN workspace.railpulse_gold.loaded_cycles AS cycles USING (loaded_cycle_id)
),
lineage_violations AS (
  SELECT COUNT(*) AS source_lineage_mismatch_count
  FROM (
    SELECT DISTINCT
      snapshots.dataset_version,
      snapshots.telemetry_source_sha256,
      snapshots.telemetry_ingestion_batch_id
    FROM workspace.railpulse_gold.feature_snapshots_v2 AS snapshots
    LEFT ANTI JOIN (
      SELECT DISTINCT dataset_version, source_sha256, ingestion_batch_id
      FROM workspace.railpulse_silver.telemetry_accepted
    ) AS telemetry_lineage
    ON snapshots.dataset_version = telemetry_lineage.dataset_version
      AND snapshots.telemetry_source_sha256 = telemetry_lineage.source_sha256
      AND snapshots.telemetry_ingestion_batch_id = telemetry_lineage.ingestion_batch_id
  )
),
label_columns AS (
  SELECT COUNT(*) AS label_or_target_column_count
  FROM workspace.information_schema.columns
  WHERE table_schema = 'railpulse_gold'
    AND table_name = 'feature_snapshots_v2'
    AND lower(column_name) RLIKE 'failure|label|target'
)
SELECT * FROM snapshot_violations
CROSS JOIN boundary_violations
CROSS JOIN lineage_violations
CROSS JOIN label_columns;
