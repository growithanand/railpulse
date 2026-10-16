-- Inventory must contain exactly the expected loaded_cycles table after the first managed run.
SHOW TABLES IN workspace.railpulse_gold;

-- Confirm managed Delta metadata and the nine-column loaded-cycle contract.
DESCRIBE TABLE EXTENDED workspace.railpulse_gold.loaded_cycles;

-- Identity and complete-table reconciliation. Expected values are documented in
-- docs/managed_gold_first_run.md.
SELECT
    COUNT(*) AS total_cycle_count,
    COUNT(loaded_cycle_id) AS non_null_cycle_ids,
    COUNT(DISTINCT loaded_cycle_id) AS unique_cycle_ids,
    SUM(loaded_observation_count) AS loaded_observation_count,
    COUNT(observed_duration_seconds) AS duration_count,
    COUNT_IF(is_right_censored) AS right_censored_count
FROM workspace.railpulse_gold.loaded_cycles;

-- The four visible-start/right-censoring groups must reconcile to the complete table.
SELECT
    loaded_cycle_start_type,
    is_right_censored,
    COUNT(*) AS cycle_count,
    SUM(loaded_observation_count) AS loaded_observation_count,
    COUNT(observed_duration_seconds) AS duration_count
FROM workspace.railpulse_gold.loaded_cycles
GROUP BY
    loaded_cycle_start_type,
    is_right_censored
ORDER BY
    loaded_cycle_start_type,
    is_right_censored;

-- Every result must be zero. These are structural cycle-contract checks, not health rules.
SELECT
    COUNT_IF(
        loaded_cycle_id IS NULL
        OR loaded_cycle_start_record_id IS NULL
        OR loaded_cycle_start_timestamp IS NULL
        OR loaded_observation_count IS NULL
        OR is_right_censored IS NULL
    ) AS missing_required_value_count,
    COUNT_IF(
        loaded_cycle_id != SHA2(
            CONCAT_WS('|', 'loaded-cycle-v1', loaded_cycle_start_record_id),
            256
        )
    ) AS invalid_deterministic_cycle_id_count,
    COUNT_IF(loaded_observation_count <= 0) AS non_positive_observation_count,
    COUNT_IF(
        loaded_cycle_start_type IS NULL
        OR loaded_cycle_start_type NOT IN ('observed', 'left_censored')
    )
        AS unsupported_start_type_count,
    COUNT_IF(
        is_right_censored
        AND (
            loaded_cycle_stop_record_id IS NOT NULL
            OR loaded_cycle_stop_timestamp IS NOT NULL
            OR observed_duration_seconds IS NOT NULL
        )
    ) AS censored_cycle_with_stop_evidence_count,
    COUNT_IF(
        NOT is_right_censored
        AND (
            loaded_cycle_stop_record_id IS NULL
            OR loaded_cycle_stop_timestamp IS NULL
            OR observed_duration_seconds IS NULL
        )
    ) AS settled_cycle_without_stop_evidence_count,
    COUNT_IF(
        NOT is_right_censored
        AND (
            observed_duration_seconds <= 0
            OR observed_duration_seconds != TIMESTAMPDIFF(
                SECOND,
                loaded_cycle_start_timestamp,
                loaded_cycle_stop_timestamp
            )
        )
    ) AS invalid_observed_duration_count
FROM workspace.railpulse_gold.loaded_cycles;
