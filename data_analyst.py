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
    WITH customer_traffic_stats AS (
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

            -- Extract hour from timestamp for traffic pattern analysis
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 
                CAST(JSON_EXTRACT(ci.contract, '$.peak_start_time') AS INTEGER) AND 
                CAST(JSON_EXTRACT(ci.contract, '$.peak_end_time') AS INTEGER) 
                THEN 1 END) as peak_hour_transactions,

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

            -- Actual peak times analysis - find the hour with most traffic
            MODE() WITHIN GROUP (ORDER BY EXTRACT(hour FROM timestamp)) as actual_peak_hour,

            -- Calculate traffic distribution across hours
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 0 AND 5 THEN 1 END) as night_traffic_0_5,
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 6 AND 11 THEN 1 END) as morning_traffic_6_11,
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 12 AND 17 THEN 1 END) as afternoon_traffic_12_17,
            COUNT(CASE WHEN EXTRACT(hour FROM timestamp) BETWEEN 18 AND 23 THEN 1 END) as evening_traffic_18_23,

            MIN(ct.timestamp) as first_transaction,
            MAX(ct.timestamp) as last_transaction

        FROM customer_traffic ct
        JOIN customer_info ci ON ct.customer_api_key = ci.customer_api_key
        GROUP BY ct.customer_api_key, ci.customer_name, ci.tier, ci.allocated_tps, ci.contract
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

            -- Peak hour usage pattern
            peak_hour_transactions * 100.0 / total_transactions as peak_hour_percentage,

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
                'actual_peak_start_time', actual_peak_hour,
                'actual_peak_end_time', actual_peak_hour + 1,
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
        peak_hour_percentage,
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

        -- Extract actual contract details
        CAST(JSON_EXTRACT(actual_contract, '$.actual_peak_start_time') AS INTEGER) as actual_peak_start,
        CAST(JSON_EXTRACT(actual_contract, '$.actual_peak_end_time') AS INTEGER) as actual_peak_end,
        JSON_EXTRACT(actual_contract, '$.contract_vs_actual_alignment') as peak_alignment

    FROM customer_analysis;
    """

    # Create carrier_profile table with actual peak times analysis
    create_carrier_profile_query = """
    CREATE OR REPLACE TABLE carrier_profile AS
    WITH carrier_traffic_stats AS (
        SELECT 
            cc.carrier_name,
            cc.email,
            cc.allowed_tps,
            cc.countries_supported,

            -- Traffic statistics
            COALESCE(COUNT(ct.customer_api_key), 0) as total_transactions_handled,
            COALESCE(COUNT(ct.customer_api_key), 0) / (24.0 * 60 * 60) as avg_tps_actual,
            COALESCE(COUNT(CASE WHEN ct.status = 'SUCCESS' THEN 1 END), 0) as successful_deliveries,
            COALESCE(COUNT(CASE WHEN ct.status = 'FAILED' THEN 1 END), 0) as failed_deliveries,
            CASE 
                WHEN COUNT(ct.customer_api_key) = 0 THEN 0
                ELSE COUNT(CASE WHEN ct.status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(ct.customer_api_key)
            END as delivery_success_rate,

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

            -- Find actual peak hour
            CASE 
                WHEN COUNT(ct.customer_api_key) > 0 
                THEN MODE() WITHIN GROUP (ORDER BY EXTRACT(hour FROM ct.timestamp))
                ELSE NULL
            END as actual_peak_hour,

            MIN(ct.timestamp) as first_transaction_date,
            MAX(ct.timestamp) as last_transaction_date

        FROM carrier_capacity cc
        LEFT JOIN customer_traffic ct ON cc.carrier_name = ct.carrier_name
        GROUP BY cc.carrier_name, cc.email, cc.allowed_tps, cc.countries_supported
    ),

    carrier_analysis AS (
        SELECT *,
            -- Capacity utilization
            CASE 
                WHEN avg_tps_actual > allowed_tps * 0.9 THEN 'OVER_CAPACITY'
                WHEN avg_tps_actual > allowed_tps * 0.7 THEN 'HIGH_UTILIZATION'
                WHEN avg_tps_actual > allowed_tps * 0.3 THEN 'MEDIUM_UTILIZATION'
                ELSE 'LOW_UTILIZATION'
            END as capacity_utilization_level,

            -- Performance rating
            CASE 
                WHEN delivery_success_rate > 95 THEN 'EXCELLENT'
                WHEN delivery_success_rate > 90 THEN 'GOOD'
                WHEN delivery_success_rate > 80 THEN 'AVERAGE'
                ELSE 'POOR'
            END as performance_rating,

            -- Business hours vs off hours ratio
            business_hours_traffic * 100.0 / NULLIF(total_transactions_handled, 0) as business_hours_percentage,

            -- Days active
            CASE 
                WHEN first_transaction_date IS NOT NULL 
                THEN DATE_DIFF('day', first_transaction_date, last_transaction_date) + 1 
                ELSE 0 
            END as days_active,

            -- Average daily transactions
            CASE 
                WHEN first_transaction_date IS NOT NULL AND total_transactions_handled > 0
                THEN total_transactions_handled / (DATE_DIFF('day', first_transaction_date, last_transaction_date) + 1.0)
                ELSE 0
            END as avg_daily_transactions,

            -- Determine actual peak period
            CASE 
                WHEN total_transactions_handled = 0 THEN 'NO_TRAFFIC'
                WHEN night_traffic_0_5 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'NIGHT_0_5'
                WHEN morning_traffic_6_11 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'MORNING_6_11'
                WHEN afternoon_traffic_12_17 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'AFTERNOON_12_17'
                ELSE 'EVENING_18_23'
            END as actual_peak_period,

            -- Create actual peak times JSON
            JSON_OBJECT(
                'traffic_start_time', 0,
                'traffic_end_time', 23,
                'peak_start_time', COALESCE(actual_peak_hour, 12),
                'peak_end_time', COALESCE(actual_peak_hour + 7, 19),  -- 8-hour peak window similar to contract example
                'actual_peak_hour', actual_peak_hour,
                'actual_peak_period', 
                    CASE 
                        WHEN total_transactions_handled = 0 THEN 'NO_TRAFFIC'
                        WHEN night_traffic_0_5 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'NIGHT_0_5'
                        WHEN morning_traffic_6_11 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'MORNING_6_11'
                        WHEN afternoon_traffic_12_17 = GREATEST(night_traffic_0_5, morning_traffic_6_11, afternoon_traffic_12_17, evening_traffic_18_23) THEN 'AFTERNOON_12_17'
                        ELSE 'EVENING_18_23'
                    END,
                'night_traffic_pct', ROUND(night_traffic_0_5 * 100.0 / NULLIF(total_transactions_handled, 0), 2),
                'morning_traffic_pct', ROUND(morning_traffic_6_11 * 100.0 / NULLIF(total_transactions_handled, 0), 2),
                'afternoon_traffic_pct', ROUND(afternoon_traffic_12_17 * 100.0 / NULLIF(total_transactions_handled, 0), 2),
                'evening_traffic_pct', ROUND(evening_traffic_18_23 * 100.0 / NULLIF(total_transactions_handled, 0), 2)
            ) as actual_peak_times

        FROM carrier_traffic_stats
    )

    SELECT 
        carrier_name,
        email,
        allowed_tps,
        avg_tps_actual,
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
        actual_peak_times,

        -- Extract peak times for easy access
        CAST(JSON_EXTRACT(actual_peak_times, '$.peak_start_time') AS INTEGER) as peak_start_time,
        CAST(JSON_EXTRACT(actual_peak_times, '$.peak_end_time') AS INTEGER) as peak_end_time,
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
    print("\n🔍 Sample Customer Profile Data (with actual contract):")
    print(conn.execute(
        "SELECT customer_name, tier, actual_peak_hour, actual_peak_period, peak_alignment, contract_compliance FROM customer_profile LIMIT 3").df())

    print("\n🔍 Sample Carrier Profile Data (with peak times):")
    print(conn.execute(
        "SELECT carrier_name, actual_peak_hour, actual_peak_period, night_traffic_pct, morning_traffic_pct, afternoon_traffic_pct, evening_traffic_pct FROM carrier_profile LIMIT 3").df())

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