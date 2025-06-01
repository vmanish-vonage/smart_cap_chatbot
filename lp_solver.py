import ast
import duckdb
from scipy.optimize import linprog
import numpy as np
import pandas as pd

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)        # Don't wrap lines


def allocate_customer_capacity(customer_api_key, request: dict):
    con = duckdb.connect('traffic_data.duckdb')

    profile_df = con.execute("""
        SELECT carrier_name, 
               allowed_tps AS profile_allowed_tps, 
               avg_tps_actual AS profile_actual_tps, 
               actual_peak_start_time, 
               actual_peak_end_time, 
               allocatable_tps,
               supported_countries_list 
        FROM carrier_profile
    """).fetchdf()

    profile_df['supported_countries_list'] = profile_df['supported_countries_list'].apply(ast.literal_eval)

    # Filter carriers that support at least one requested destination country
    filtered_carriers = profile_df[
        profile_df['supported_countries_list'].apply(
            lambda countries: any(dest in countries for dest in request['destinations'])
        )
    ].copy()

    if filtered_carriers.empty:
        return {'status': 'error', 'message': 'No carriers found supporting the requested destinations'}

    def compute_allocatable_tps(row):
        try:
            allocatable_tps = int(row['allocatable_tps'])
            return max(0, allocatable_tps)
        except Exception as e:
            print(f"Row error: {row.to_dict()}, Error: {e}")
            return 0

    filtered_carriers['max_allocatable_tps'] = filtered_carriers.apply(compute_allocatable_tps, axis=1)

    if filtered_carriers.empty:
        return {'status': 'error', 'message': 'No carriers found supporting the requested TPS'}

    # New filter: peak time overlap

    requested_start_str, requested_end_str = request.get('peak_window', '0-23').split('-')
    requested_start = int(requested_start_str)
    requested_end = int(requested_end_str)

    def peak_time_overlaps(row):
        carrier_start = int(row['actual_peak_start_time'])
        carrier_end = int(row['actual_peak_end_time'])
        # Check if time windows overlap (simple numeric overlap)
        return not (carrier_end <= requested_start or carrier_start >= requested_end)

    filtered_carriers = filtered_carriers[filtered_carriers.apply(peak_time_overlaps, axis=1)]

    if filtered_carriers.empty:
        return {'status': 'error', 'message': 'No carriers found supporting the requested peak times'}

    print("Filtered by country and peak time:")
    print(filtered_carriers)

    tps_limits = filtered_carriers['max_allocatable_tps'].values
    num_carriers = len(filtered_carriers)
    destinations = request['destinations']
    num_dest = len(destinations)

    # Objective: minimize total allocated TPS (or any other objective)
    c = [1.0] * num_carriers

    # Equality constraint: sum of allocations = requested TPS
    A_eq = [ [1.0]*num_carriers ]
    b_eq = [request['requested_tps']]

    # Inequality constraints for each destination:
    # For each destination d, sum of allocations from carriers supporting d >= demand_per_dest
    demand_per_dest = request['requested_tps'] / num_dest

    A_ub = []
    b_ub = []

    # For linprog, inequalities are A_ub x <= b_ub
    # So to encode sum of allocations for d >= demand_per_dest
    # we rewrite as -sum >= -demand_per_dest => sum <= demand_per_dest with negative sign flipped

    for d in destinations:
        row = []
        for idx, countries in enumerate(filtered_carriers['supported_countries_list']):
            # If carrier supports d, coefficient is -1 (to flip inequality), else 0
            row.append(-1.0 if d in countries else 0.0)
        A_ub.append(row)
        b_ub.append(-demand_per_dest)

    bounds = [(0, tps_limits[i]) for i in range(num_carriers)]

    result = linprog(c=c, A_ub=A_ub, b_ub=b_ub, A_eq=A_eq, b_eq=b_eq, bounds=bounds, method='highs')

    if not result.success:
        return {'status': 'error', 'message': 'Could not allocate TPS under current constraints'}

    allocations = []
    for idx, tps in enumerate(result.x):
        if tps > 0:
            allocations.append({
                'carrier': filtered_carriers.iloc[idx]['carrier_name'],
                'allocated_tps': round(float(tps), 2)
            })

    #Success Scenario
    update_allocatable_tps(con, allocations)

    return {
        'status': 'success',
        'total_requested_tps': int(request['requested_tps']),
        'total_allocated_tps': round(float(sum(result.x)), 2),
        'allocations': allocations
    }


def update_allocatable_tps(con, allocations):
    for allocation in allocations:
        carrier = allocation['carrier']
        allocated_tps = allocation['allocated_tps']

        # Subtract allocated TPS from allocatable_tps
        con.execute(f"""
            UPDATE carrier_profile
            SET allocatable_tps = GREATEST(allocatable_tps - {allocated_tps}, 0)
            WHERE carrier_name = '{carrier}'
        """)

# def main():
#     request = {
#         "requested_tps": 50,
#         "destinations": ["US", "CA"],
#         "traffic_volume": 100000,
#         "peak_window": "10-12",
#         "peak_tps": 300
#     }
#
#     result = allocate_customer_capacity("", request)
#     print("Allocation Result:")
#     print(result)
#
# if __name__ == "__main__":
#     main()
