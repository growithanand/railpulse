SELECT
    loaded_cycle_start_type,
    is_right_censored,
    COUNT(*) AS cycle_count,
    SUM(loaded_observation_count) AS loaded_observation_count,
    COUNT(observed_duration_seconds) AS duration_cycle_count,
    MIN(observed_duration_seconds) AS minimum_observed_duration_seconds,
    PERCENTILE_APPROX(observed_duration_seconds, 0.5, 10000)
        AS median_observed_duration_seconds,
    PERCENTILE_APPROX(observed_duration_seconds, 0.95, 10000)
        AS p95_observed_duration_seconds,
    MAX(observed_duration_seconds) AS maximum_observed_duration_seconds
FROM gold_loaded_cycles
GROUP BY
    loaded_cycle_start_type,
    is_right_censored
ORDER BY
    loaded_cycle_start_type,
    is_right_censored
