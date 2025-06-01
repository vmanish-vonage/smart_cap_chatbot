import duckdb
import json
from datetime import datetime, timedelta
import pandas as pd


def preprocess_data():
    """
    Create helper tables for analysis:
    - customer_profile: Actual usage patterns vs contracted terms with actual contract analysis
    - carrier_profile: Actual carrier performance and utilization with peak times analysis
    """

    print("In preprocessing")

    conn = duckdb.connect('traffic_data.duckdb')

    # Create customer_profile table with actual contract analysis
    create_customer_profile_query = """
    CREATE OR REPLACE TABLE customer_profile AS
    WITH customer_hourly_traffic AS (
        SELECT 
            ct.customer_api_key,
            EXTRACT(hour FROM ct.timestamp) as hour,
            COUNT(*) as transactions_per_hour,
            COUNT(CASE WHEN ct.status = 'SUCCESS' THEN 1 END) as successful_per_hour
        FROM customer_traffic ct
        GROUP BY ct.customer_api_key, EXTRACT(hour FROM ct.timestamp)
    ),

    customer_peak_detection AS (
        SELECT 
            customer_api_key,
            -- Find the hour with maximum traffic (actual peak hour)
            (SELECT hour FROM customer_hourly_traffic cht2 
             WHERE cht2.customer_api_key = cht.customer_api_key 
             ORDER BY transactions_per_hour DESC LIMIT 1) as actual_peak_hour,
            -- Get max transactions in any hour converted to TPS (peak_tps)
            MAX(transactions_per_hour) / 3600.0 as peak_tps,
            -- Calculate average transactions per hour converted to TPS
            AVG(transactions_per_hour) / 3600.0 as avg_tps
        FROM customer_hourly_traffic cht
        GROUP BY customer_api_key
    ),

    customer_traffic_stats AS (
        SELECT 
            ct.customer_api_key,
            ci.customer_name,
            ci.tier,
            ci.allocated_tps,
            ci.contract,
            COUNT(*) as total_transactions,
            COUNT(*) / (24.0 * 60 * 60) as avg_tps_used,  -- Assuming data spans multiple days
            COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_transactions,
            COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_transactions,
            COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*) as success_rate,

            -- Get actual peak hour and peak TPS from peak detection
            cpd.actual_peak_hour,
            cpd.peak_tps,
            cpd.avg_tps,

            -- Calculate peak period traffic (8-hour window around peak hour)
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 
                GREATEST(0, cpd.actual_peak_hour - 4) AND 
                LEAST(23, cpd.actual_peak_hour + 3) 
                THEN 1 END) as actual_peak_period_transactions,

            -- Extract hour from timestamp for traffic pattern analysis with CONTRACT times
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 
                CAST(JSON_EXTRACT(ci.contract, '$.peak_start_time') AS INTEGER) AND 
                CAST(JSON_EXTRACT(ci.contract, '$.peak_end_time') AS INTEGER) 
                THEN 1 END) as contract_peak_hour_transactions,

            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 
                CAST(JSON_EXTRACT(ci.contract, '$.traffic_start_time') AS INTEGER) AND 
                CAST(JSON_EXTRACT(ci.contract, '$.traffic_end_time') AS INTEGER) 
                THEN 1 END) as contracted_hour_transactions,

            -- Unique carriers and countries used
            COUNT(DISTINCT ct.carrier_name) as carriers_used,
            COUNT(DISTINCT ct.destination_country) as countries_reached,

            -- Most used carrier and country
            MODE() WITHIN GROUP (ORDER BY ct.carrier_name) as primary_carrier,
            MODE() WITHIN GROUP (ORDER BY ct.destination_country) as primary_country,

            -- Calculate traffic distribution across hours
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 0 AND 5 THEN 1 END) as night_traffic_0_5,
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 6 AND 11 THEN 1 END) as morning_traffic_6_11,
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 12 AND 17 THEN 1 END) as afternoon_traffic_12_17,
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 18 AND 23 THEN 1 END) as evening_traffic_18_23,

            MIN(ct.timestamp) as first_transaction,
            MAX(ct.timestamp) as last_transaction

        FROM customer_traffic ct
        JOIN customer_info ci ON ct.customer_api_key = ci.customer_api_key
        JOIN customer_peak_detection cpd ON ct.customer_api_key = cpd.customer_api_key
        GROUP BY ct.customer_api_key, ci.customer_name, ci.tier, ci.allocated_tps, ci.contract, 
                 cpd.actual_peak_hour, cpd.peak_tps, cpd.avg_tps
    ),

    customer_analysis AS (
        SELECT *,
            -- Calculate actual contract compliance
            CASE 
                WHEN contracted_hour_transactions * 100.0 / total_transactions > 80 THEN 'COMPLIANT'
                WHEN contracted_hour_transactions * 100.0 / total_transactions > 50 THEN 'PARTIALLY_COMPLIANT'
                ELSE 'NON_COMPLIANT'
            END as contract_compliance,

            -- TPS utilization analysis
            CASE 
                WHEN avg_tps_used > allocated_tps * 0.9 THEN 'HIGH_UTILIZATION'
                WHEN avg_tps_used > allocated_tps * 0.5 THEN 'MEDIUM_UTILIZATION'
                ELSE 'LOW_UTILIZATION'
            END as tps_utilization_level,

            -- Peak hour usage pattern (using ACTUAL peak period)
            actual_peak_period_transactions * 100.0 / total_transactions as peak_period_percentage,

            -- Calculate days active
            DATE_DIFF('day', first_transaction, last_transaction) + 1 as days_active,

            -- Actual daily TPS
            total_transactions / (DATE_DIFF('day', first_transaction, last_transaction) + 1.0) as actual_daily_tps,

            -- Determine actual peak period based on traffic distribution
            CASE 
                WHEN night_traffic_0_5 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'NIGHT_0_5'
                WHEN morning_traffic_6_11 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'MORNING_6_11'
                WHEN afternoon_traffic_12_17 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'AFTERNOON_12_17'
                ELSE 'EVENING_18_23'
            END as actual_peak_period,

            -- Calculate ACTUAL peak start and end times (8-hour window around peak hour)
            GREATEST(0, actual_peak_hour - 4) as actual_peak_start_time,
            LEAST(23, actual_peak_hour + 3) as actual_peak_end_time,

            -- Create actual contract JSON based on observed behavior
            JSON_OBJECT(
                'actual_traffic_start_time', 
                    CASE WHEN total_transactions > 0 THEN 
                        (SELECT MIN(EXTRACT(hour FROM timestamp)) FROM customer_traffic ct2 WHERE ct2.customer_api_key = customer_api_key)
                    ELSE 0 END,
                'actual_traffic_end_time', 
                    CASE WHEN total_transactions > 0 THEN 
                        (SELECT MAX(EXTRACT(hour FROM timestamp)) FROM customer_traffic ct2 WHERE ct2.customer_api_key = customer_api_key)
                    ELSE 23 END,
                'actual_peak_start_time', GREATEST(0, actual_peak_hour - 4),
                'actual_peak_end_time', LEAST(23, actual_peak_hour + 3),
                'actual_peak_hour', actual_peak_hour,
                'peak_tps', peak_tps,
                'avg_tps', avg_tps,
                'actual_peak_period', actual_peak_period,
                'contract_vs_actual_alignment', 
                    CASE 
                        WHEN actual_peak_hour BETWEEN CAST(JSON_EXTRACT(contract, '$.peak_start_time') AS INTEGER) 
                                                  AND CAST(JSON_EXTRACT(contract, '$.peak_end_time') AS INTEGER) 
                        THEN 'ALIGNED'
                        ELSE 'MISALIGNED'
                    END
            ) as actual_contract

        FROM customer_traffic_stats
    )

    SELECT 
        customer_api_key,
        customer_name,
        tier,
        allocated_tps,
        avg_tps_used,
        actual_daily_tps,
        total_transactions,
        successful_transactions,
        failed_transactions,
        success_rate,
        contract_compliance,
        tps_utilization_level,
        peak_period_percentage,
        peak_tps,
        avg_tps,
        carriers_used,
        countries_reached,
        primary_carrier,
        primary_country,
        days_active,
        first_transaction,
        last_transaction,
        actual_peak_hour,
        actual_peak_period,
        actual_contract,

        -- Extract contract details for easy access
        CAST(JSON_EXTRACT(contract, '$.traffic_start_time') AS INTEGER) as contract_start_hour,
        CAST(JSON_EXTRACT(contract, '$.traffic_end_time') AS INTEGER) as contract_end_hour,
        CAST(JSON_EXTRACT(contract, '$.peak_start_time') AS INTEGER) as contract_peak_start,
        CAST(JSON_EXTRACT(contract, '$.peak_end_time') AS INTEGER) as contract_peak_end,

        -- Extract ACTUAL contract details (computed from traffic)
        actual_peak_start_time,
        actual_peak_end_time,
        JSON_EXTRACT(actual_contract, '$.contract_vs_actual_alignment') as peak_alignment

    FROM customer_analysis;
    """

    # Create carrier_profile table with actual peak times analysis and allocatable_tps
    create_carrier_profile_query = """
    CREATE OR REPLACE TABLE carrier_profile AS
    WITH carrier_hourly_traffic AS (
        SELECT 
            ct.carrier_name,
            EXTRACT(hour FROM ct.timestamp) as hour,
            COUNT(*) as transactions_per_hour,
            COUNT(CASE WHEN ct.status = 'SUCCESS' THEN 1 END) as successful_per_hour
        FROM customer_traffic ct
        GROUP BY ct.carrier_name, EXTRACT(hour FROM ct.timestamp)
    ),

    carrier_peak_detection AS (
        SELECT 
            carrier_name,
            -- Find the hour with maximum traffic (actual peak hour)
            (SELECT hour FROM carrier_hourly_traffic cht2 
             WHERE cht2.carrier_name = cht.carrier_name 
             ORDER BY transactions_per_hour DESC LIMIT 1) as actual_peak_hour,
            -- Get max transactions in any hour converted to TPS (peak_tps)
            MAX(transactions_per_hour) / 3600.0 as peak_tps,
            -- Calculate average transactions per hour converted to TPS
            AVG(transactions_per_hour) / 3600.0 as avg_tps
        FROM carrier_hourly_traffic cht
        GROUP BY carrier_name
    ),

    carrier_traffic_stats AS (
        SELECT 
            cc.carrier_name,
            cc.email,
            cc.allowed_tps,
            cc.countries_supported,

            -- Traffic statistics
            COALESCE(COUNT(ct.customer_api_key), 0) as total_transactions_handled,
            CAST(COALESCE(COUNT(ct.customer_api_key), 0) / (24.0 * 60 * 60) AS INTEGER) as avg_tps_actual,
            COALESCE(COUNT(CASE WHEN ct.status = 'SUCCESS' THEN 1 END), 0) as successful_deliveries,
            COALESCE(COUNT(CASE WHEN ct.status = 'FAILED' THEN 1 END), 0) as failed_deliveries,
            CASE 
                WHEN COUNT(ct.customer_api_key) = 0 THEN 0
                ELSE COUNT(CASE WHEN ct.status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(ct.customer_api_key)
            END as delivery_success_rate,

            -- Get actual peak hour and peak TPS from peak detection
            COALESCE(cpd.actual_peak_hour, 12) as actual_peak_hour,  -- Default to noon if no traffic
            COALESCE(cpd.peak_tps, 0) as peak_tps,
            COALESCE(cpd.avg_tps, 0) as avg_tps,

            -- Customer diversity
            COUNT(DISTINCT ct.customer_api_key) as unique_customers_served,
            COUNT(DISTINCT ct.destination_country) as countries_actually_served,

            -- Geographic analysis
            CASE 
                WHEN COUNT(ct.destination_country) > 0 
                THEN MODE() WITHIN GROUP (ORDER BY ct.destination_country)
                ELSE NULL
            END as primary_destination_country,

            -- Time pattern analysis with actual peak times
            COUNT(CASE WHEN EXTRACT(hour FROM ct.timestamp) BETWEEN 9 AND 17 THEN 1 END) as business_hours_traffic,
            COUNT(CASE WHEN EXTRACT(hour FROM ct.timestamp) BETWEEN 18 AND 23 OR EXTRACT(hour FROM ct.timestamp) BETWEEN 0 AND 8 THEN 1 END) as off_hours_traffic,

            -- Detailed hourly analysis for peak detection
            COUNT(CASE WHEN EXTRACT(hour FROM ct.timestamp) BETWEEN 0 AND 5 THEN 1 END) as night_traffic_0_5,
            COUNT(CASE WHEN EXTRACT(hour FROM ct.timestamp) BETWEEN 6 AND 11 THEN 1 END) as morning_traffic_6_11,
            COUNT(CASE WHEN EXTRACT(hour FROM ct.timestamp) BETWEEN 12 AND 17 THEN 1 END) as afternoon_traffic_12_17,
            COUNT(CASE WHEN EXTRACT(hour FROM ct.timestamp) BETWEEN 18 AND 23 THEN 1 END) as evening_traffic_18_23,

            MIN(ct.timestamp) as first_transaction_date,
            MAX(ct.timestamp) as last_transaction_date

        FROM carrier_capacity cc
        LEFT JOIN customer_traffic ct ON cc.carrier_name = ct.carrier_name
        LEFT JOIN carrier_peak_detection cpd ON cc.carrier_name = cpd.carrier_name
        GROUP BY cc.carrier_name, cc.email, cc.allowed_tps, cc.countries_supported,
                 cpd.actual_peak_hour, cpd.peak_tps, cpd.avg_tps
    ),

    carrier_allocated_tps AS (
        SELECT 
            carrier_name,
            COALESCE(SUM(allocated_tps), 0) as total_allocated_tps
        FROM (
            SELECT 
                JSON_EXTRACT(value, '$.carrier') as carrier_name,
                CAST(JSON_EXTRACT(value, '$.allocated_tps') AS REAL) as allocated_tps
            FROM allocations a,
            JSON_EACH(CAST(a.allocation_description AS JSON))
            WHERE a.allocation_status = 'Approved'
        ) allocated_data
        GROUP BY carrier_name
    ),

    carrier_analysis AS (
        SELECT cts.*,
            -- Calculate ACTUAL peak start and end times (8-hour window around peak hour)
            GREATEST(0, cts.actual_peak_hour - 4) as actual_peak_start_time,
            LEAST(23, cts.actual_peak_hour + 3) as actual_peak_end_time,

            -- Calculate allocatable TPS (available capacity) - subtract allocated TPS
            CAST(GREATEST(0, cts.allowed_tps - cts.avg_tps_actual - COALESCE(cat.total_allocated_tps, 0)) AS INTEGER) as allocatable_tps,

            -- Capacity utilization (now includes allocated TPS)
            CASE 
                WHEN cts.avg_tps_actual + COALESCE(cat.total_allocated_tps, 0) > cts.allowed_tps * 0.9 THEN 'OVER_CAPACITY'
                WHEN cts.avg_tps_actual + COALESCE(cat.total_allocated_tps, 0) > cts.allowed_tps * 0.7 THEN 'HIGH_UTILIZATION'
                WHEN cts.avg_tps_actual + COALESCE(cat.total_allocated_tps, 0) > cts.allowed_tps * 0.3 THEN 'MEDIUM_UTILIZATION'
                ELSE 'LOW_UTILIZATION'
            END as capacity_utilization_level,

            -- Performance rating
            CASE 
                WHEN cts.delivery_success_rate > 95 THEN 'EXCELLENT'
                WHEN cts.delivery_success_rate > 90 THEN 'GOOD'
                WHEN cts.delivery_success_rate > 80 THEN 'AVERAGE'
                ELSE 'POOR'
            END as performance_rating,

            -- Business hours vs off hours ratio
            cts.business_hours_traffic * 100.0 / NULLIF(cts.total_transactions_handled, 0) as business_hours_percentage,

            -- Days active
            CASE 
                WHEN cts.first_transaction_date IS NOT NULL 
                THEN DATE_DIFF('day', cts.first_transaction_date, cts.last_transaction_date) + 1 
                ELSE 0 
            END as days_active,

            -- Average daily transactions
            CASE 
                WHEN cts.first_transaction_date IS NOT NULL AND cts.total_transactions_handled > 0
                THEN cts.total_transactions_handled / (DATE_DIFF('day', cts.first_transaction_date, cts.last_transaction_date) + 1.0)
                ELSE 0
            END as avg_daily_transactions,

            -- Determine actual peak period
            CASE 
                WHEN cts.total_transactions_handled = 0 THEN 'NO_TRAFFIC'
                WHEN cts.night_traffic_0_5 = GREATEST(cts.night_traffic_0_5, cts.morning_traffic_6_11, cts.afternoon_traffic_12_17, cts.evening_traffic_18_23) THEN 'NIGHT_0_5'
                WHEN cts.morning_traffic_6_11 = GREATEST(cts.night_traffic_0_5, cts.morning_traffic_6_11, cts.afternoon_traffic_12_17, cts.evening_traffic_18_23) THEN 'MORNING_6_11'
                WHEN cts.afternoon_traffic_12_17 = GREATEST(cts.night_traffic_0_5, cts.morning_traffic_6_11, cts.afternoon_traffic_12_17, cts.evening_traffic_18_23) THEN 'AFTERNOON_12_17'
                ELSE 'EVENING_18_23'
            END as actual_peak_period,

            -- Store allocated TPS for reference
            COALESCE(cat.total_allocated_tps, 0) as total_allocated_tps,

            -- Create actual peak times JSON with computed values
            JSON_OBJECT(
                'traffic_start_time', 0,
                'traffic_end_time', 23,
                'actual_peak_start_time', GREATEST(0, cts.actual_peak_hour - 4),
                'actual_peak_end_time', LEAST(23, cts.actual_peak_hour + 3),
                'actual_peak_hour', cts.actual_peak_hour,
                'peak_tps', cts.peak_tps,
                'avg_tps', cts.avg_tps,
                'actual_peak_period', 
                    CASE 
                        WHEN cts.total_transactions_handled = 0 THEN 'NO_TRAFFIC'
                        WHEN cts.night_traffic_0_5 = GREATEST(cts.night_traffic_0_5, cts.morning_traffic_6_11, cts.afternoon_traffic_12_17, cts.evening_traffic_18_23) THEN 'NIGHT_0_5'
                        WHEN cts.morning_traffic_6_11 = GREATEST(cts.night_traffic_0_5, cts.morning_traffic_6_11, cts.afternoon_traffic_12_17, cts.evening_traffic_18_23) THEN 'MORNING_6_11'
                        WHEN cts.afternoon_traffic_12_17 = GREATEST(cts.night_traffic_0_5, cts.morning_traffic_6_11, cts.afternoon_traffic_12_17, cts.evening_traffic_18_23) THEN 'AFTERNOON_12_17'
                        ELSE 'EVENING_18_23'
                    END,
                'night_traffic_pct', ROUND(cts.night_traffic_0_5 * 100.0 / NULLIF(cts.total_transactions_handled, 0), 2),
                'morning_traffic_pct', ROUND(cts.morning_traffic_6_11 * 100.0 / NULLIF(cts.total_transactions_handled, 0), 2),
                'afternoon_traffic_pct', ROUND(cts.afternoon_traffic_12_17 * 100.0 / NULLIF(cts.total_transactions_handled, 0), 2),
                'evening_traffic_pct', ROUND(cts.evening_traffic_18_23 * 100.0 / NULLIF(cts.total_transactions_handled, 0), 2),
                'total_allocated_tps', COALESCE(cat.total_allocated_tps, 0)
            ) as actual_peak_times

        FROM carrier_traffic_stats cts
        LEFT JOIN carrier_allocated_tps cat ON cts.carrier_name = cat.carrier_name
    )

    SELECT 
        carrier_name,
        email,
        allowed_tps,
        avg_tps_actual,
        allocatable_tps,  -- NOW INTEGER and accounts for allocated TPS
        total_allocated_tps,  -- NEW: Shows how much TPS is already allocated
        total_transactions_handled,
        successful_deliveries,
        failed_deliveries,
        delivery_success_rate,
        capacity_utilization_level,
        performance_rating,
        unique_customers_served,
        countries_actually_served,
        primary_destination_country,
        business_hours_percentage,
        avg_daily_transactions,
        days_active,
        first_transaction_date,
        last_transaction_date,
        countries_supported as supported_countries_list,
        actual_peak_hour,
        actual_peak_period,
        peak_tps,  -- NEW: Max TPS (transactions per second)
        avg_tps,  -- RENAMED from avg_tps_hourly
        actual_peak_times,

        -- Extract COMPUTED peak times for easy access
        actual_peak_start_time,
        actual_peak_end_time,
        CAST(JSON_EXTRACT(actual_peak_times, '$.night_traffic_pct') AS REAL) as night_traffic_pct,
        CAST(JSON_EXTRACT(actual_peak_times, '$.morning_traffic_pct') AS REAL) as morning_traffic_pct,
        CAST(JSON_EXTRACT(actual_peak_times, '$.afternoon_traffic_pct') AS REAL) as afternoon_traffic_pct,
        CAST(JSON_EXTRACT(actual_peak_times, '$.evening_traffic_pct') AS REAL) as evening_traffic_pct

    FROM carrier_analysis
    ORDER BY total_transactions_handled DESC;
    """

    # Execute the queries
    conn.execute(create_customer_profile_query)
    conn.execute(create_carrier_profile_query)

    print("✅ Enhanced helper tables created successfully!")
    print("\n📊 Customer Profile Table Schema:")
    print(conn.execute("DESCRIBE customer_profile").df())

    print("\n📊 Carrier Profile Table Schema:")
    print(conn.execute("DESCRIBE carrier_profile").df())

    # Show sample data
    print("\n🔍 Sample Customer Profile Data (with computed peak times):")
    print(conn.execute(
        "SELECT customer_name, tier, actual_peak_hour, actual_peak_start_time, actual_peak_end_time, peak_tps, peak_alignment, contract_compliance FROM customer_profile LIMIT 3").df())

    print("\n🔍 Sample Carrier Profile Data (with computed peak times and allocatable TPS):")
    print(conn.execute(
        "SELECT carrier_name, actual_peak_hour, actual_peak_start_time, actual_peak_end_time, peak_tps, allocatable_tps, night_traffic_pct, morning_traffic_pct, afternoon_traffic_pct, evening_traffic_pct FROM carrier_profile LIMIT 3").df())

    conn.close()

    print("Done Preprocessing")
    return True

# # Example usage with enhanced analytics:
# def example_usage():
#     """
#     Example of how to use the enhanced preprocess_data function
#     """
#     # Connect to DuckDB (assuming your data is already loaded)
#     conn = duckdb.connect('traffic_data.duckdb')  # or duckdb.connect() for in-memory
#
#     # Run preprocessing
#     preprocess_data(conn)
#
#     # Example analytical queries you can now run:
#
#     # 1. Customer peak alignment analysis
#     print("\n📈 Customer Peak Time Alignment Analysis:")
#     result = conn.execute("""
#         SELECT
#             peak_alignment,
#             COUNT(*) as customer_count,
#             ROUND(AVG(success_rate), 2) as avg_success_rate,
#             ROUND(AVG(peak_hour_percentage), 2) as avg_peak_usage_pct,
#             MODE() WITHIN GROUP (ORDER BY actual_peak_period) as common_actual_peak_period
#         FROM customer_profile
#         GROUP BY peak_alignment
#         ORDER BY customer_count DESC
#     """).df()
#     print(result)
#
#     # 2. Carrier peak times analysis
#     print("\n📈 Carrier Peak Times Distribution:")
#     result = conn.execute("""
#         SELECT
#             carrier_name,
#             actual_peak_hour,
#             actual_peak_period,
#             ROUND(delivery_success_rate, 2) as success_rate_pct,
#             night_traffic_pct,
#             morning_traffic_pct,
#             afternoon_traffic_pct,
#             evening_traffic_pct,
#             total_transactions_handled
#         FROM carrier_profile
#         WHERE total_transactions_handled > 0
#         ORDER BY total_transactions_handled DESC
#         LIMIT 10
#     """).df()
#     print(result)
#
#     # 3. Peak period performance comparison
#     print("\n📈 Performance by Peak Period:")
#     result = conn.execute("""
#         SELECT
#             cp.actual_peak_period as customer_peak_period,
#             COUNT(*) as customer_count,
#             ROUND(AVG(cp.success_rate), 2) as avg_customer_success_rate,
#             ROUND(AVG(cp.peak_hour_percentage), 2) as avg_peak_usage_pct,
#             COUNT(DISTINCT cp.primary_carrier) as carriers_used_in_period
#         FROM customer_profile cp
#         GROUP BY cp.actual_peak_period
#         ORDER BY customer_count DESC
#     """).df()
#     print(result)
#
#     # 4. Contract vs actual behavior analysis
#     print("\n📈 Contract vs Actual Usage Patterns:")
#     result = conn.execute("""
#         SELECT
#             contract_compliance,
#             peak_alignment,
#             COUNT(*) as customer_count,
#             ROUND(AVG(success_rate), 2) as avg_success_rate,
#             ROUND(AVG(ABS(actual_peak_hour - contract_peak_start)), 2) as avg_peak_hour_deviation
#         FROM customer_profile
#         GROUP BY contract_compliance, peak_alignment
#         ORDER BY customer_count DESC
#     """).df()
#     print(result)
#
#
# if __name__ == "__main__":
#     example_usage()