import pandas as pd
import os

print("=== USING ONLY ACTUAL DATA FROM SOURCE FILES ===")
print("No predictions - only real data from your files!")
print()

# Read the current consolidated file (use the latest one available)
if os.path.exists('Final_Consolidated_Data_Complete_Departure.csv'):
    consolidated_df = pd.read_csv('Final_Consolidated_Data_Complete_Departure.csv')
elif os.path.exists('Final_Consolidated_Data_Complete_Clockin.csv'):
    consolidated_df = pd.read_csv('Final_Consolidated_Data_Complete_Clockin.csv')
else:
    # Fallback - create from original files
    print("Creating fresh consolidation from original files...")
    from final_with_customer_mapping import *
    consolidated_df = pd.read_csv('Final_Consolidated_Data_Updated_temp.csv')

print(f"📊 Starting with: {len(consolidated_df)} loads")

# Initialize the final dataframe with current data
final_df = consolidated_df.copy()

# 1. ACTUAL PLANNED DEPARTURE TIME DATA
print("\n1. 📋 PROCESSING PLANNED DEPARTURE TIME (ACTUAL DATA ONLY)")
if os.path.exists("1.Depot_Departures.csv"):
    depot_df = pd.read_csv("1.Depot_Departures.csv", sep='\t', encoding='utf-8', engine='python')
    print(f"   Source file has {len(depot_df)} loads with ACTUAL planned departure times")
    
    # Create lookup for actual planned departure times
    actual_planned_departures = {}
    actual_dj_departures = {}
    
    for _, row in depot_df.iterrows():
        load_name = str(row.get('Load Name', '')).strip()
        planned_dep = str(row.get('Planned Departure Time', '')).strip()
        dj_dep = str(row.get('DJ Departure Time', '')).strip()
        
        if load_name != '' and load_name != 'nan':
            if planned_dep != '' and planned_dep != 'nan':
                actual_planned_departures[load_name] = planned_dep
            if dj_dep != '' and dj_dep != 'nan':
                actual_dj_departures[load_name] = dj_dep
    
    print(f"   ✅ Found actual planned departure times for {len(actual_planned_departures)} loads")
    print(f"   ✅ Found actual DJ departure times for {len(actual_dj_departures)} loads")
    
    # Apply ONLY actual planned departure times
    planned_filled = 0
    dj_filled = 0
    
    for idx, row in final_df.iterrows():
        load_number = str(row['Load Number']).strip()
        
        # Fill Planned Departure Time with actual data only
        if load_number in actual_planned_departures:
            final_df.at[idx, 'Planned Departure Time'] = actual_planned_departures[load_number]
            planned_filled += 1
        
        # Fill DJ Departure Time with actual data only
        if load_number in actual_dj_departures:
            final_df.at[idx, 'Dj Departure Time'] = actual_dj_departures[load_number]
            dj_filled += 1
    
    print(f"   ✅ Applied {planned_filled} ACTUAL planned departure times")
    print(f"   ✅ Applied {dj_filled} ACTUAL DJ departure times")

# 2. ACTUAL CLOCKIN TIME DATA
print("\n2. 🕐 PROCESSING CLOCKIN TIME (ACTUAL DATA ONLY)")
if os.path.exists("4.Timestamps_and_Duration.csv"):
    timestamps_df = pd.read_csv("4.Timestamps_and_Duration.csv", encoding='utf-8', engine='python')
    print(f"   Source file has {len(timestamps_df)} loads with ACTUAL clockin times")
    
    # Create lookup for actual clockin times
    actual_clockin_times = {}
    actual_arrival_depot = {}
    
    for _, row in timestamps_df.iterrows():
        load_name = str(row.get('load_name', '')).strip()
        clockin_time = str(row.get('Load StartTime (Pre-Trip Start)', '')).strip()
        arrival_depot = str(row.get('ArriveAtDepot(Odo)', '')).strip()
        
        if load_name != '' and load_name != 'nan':
            if clockin_time != '' and clockin_time != 'nan':
                actual_clockin_times[load_name] = clockin_time
            if arrival_depot != '' and arrival_depot != 'nan':
                actual_arrival_depot[load_name] = arrival_depot
    
    print(f"   ✅ Found actual clockin times for {len(actual_clockin_times)} loads")
    print(f"   ✅ Found actual arrival at depot times for {len(actual_arrival_depot)} loads")
    
    # Apply ONLY actual clockin times
    clockin_filled = 0
    arrival_filled = 0
    
    for idx, row in final_df.iterrows():
        load_number = str(row['Load Number']).strip()
        
        # Fill Clockin Time with actual data only
        if load_number in actual_clockin_times:
            final_df.at[idx, 'Clockin Time'] = actual_clockin_times[load_number]
            clockin_filled += 1
        
        # Fill Arrival At Depot with actual data only
        if load_number in actual_arrival_depot:
            final_df.at[idx, 'Arrival At Depot'] = actual_arrival_depot[load_number]
            arrival_filled += 1
    
    print(f"   ✅ Applied {clockin_filled} ACTUAL clockin times")
    print(f"   ✅ Applied {arrival_filled} ACTUAL arrival at depot times")

# 3. ACTUAL CUSTOMER TIMESTAMP DATA
print("\n3. 🏢 PROCESSING CUSTOMER TIMESTAMPS (ACTUAL DATA ONLY)")
if os.path.exists("2.Customer_Timestamps.csv"):
    customer_df = pd.read_csv("2.Customer_Timestamps.csv", encoding='utf-8', engine='python')
    print(f"   Source file has {len(customer_df)} loads with ACTUAL customer timestamps")
    
    # Create lookup for actual customer timestamps
    actual_arrival_customer = {}
    actual_departure_customer = {}
    actual_service_time = {}
    
    for _, row in customer_df.iterrows():
        load_name = str(row.get('load_name', '')).strip()
        arrival_customer = str(row.get('ArrivedAtCustomer(Odo)', '')).strip()
        departure_customer = str(row.get('Offloading', '')).strip()
        service_time = str(row.get('Total Time Spent @ Customer', '')).strip()
        
        if load_name != '' and load_name != 'nan':
            if arrival_customer != '' and arrival_customer != 'nan':
                actual_arrival_customer[load_name] = arrival_customer
            if departure_customer != '' and departure_customer != 'nan':
                actual_departure_customer[load_name] = departure_customer
            if service_time != '' and service_time != 'nan':
                actual_service_time[load_name] = service_time
    
    print(f"   ✅ Found actual arrival at customer times for {len(actual_arrival_customer)} loads")
    print(f"   ✅ Found actual departure from customer times for {len(actual_departure_customer)} loads")
    print(f"   ✅ Found actual service times for {len(actual_service_time)} loads")
    
    # Apply ONLY actual customer timestamps
    arrival_cust_filled = 0
    departure_cust_filled = 0
    service_filled = 0
    
    for idx, row in final_df.iterrows():
        load_number = str(row['Load Number']).strip()
        
        # Fill customer timestamps with actual data only
        if load_number in actual_arrival_customer:
            final_df.at[idx, 'Arrival At Customer'] = actual_arrival_customer[load_number]
            arrival_cust_filled += 1
        
        if load_number in actual_departure_customer:
            final_df.at[idx, 'Departure Time From Customer'] = actual_departure_customer[load_number]
            departure_cust_filled += 1
        
        if load_number in actual_service_time:
            final_df.at[idx, 'Service Time At Customer'] = actual_service_time[load_number]
            service_filled += 1
    
    print(f"   ✅ Applied {arrival_cust_filled} ACTUAL arrival at customer times")
    print(f"   ✅ Applied {departure_cust_filled} ACTUAL departure from customer times")
    print(f"   ✅ Applied {service_filled} ACTUAL service times")

# 4. ACTUAL TIME IN ROUTE DATA
print("\n4. 🚛 PROCESSING TIME IN ROUTE (ACTUAL DATA ONLY)")
if os.path.exists("5.Time_in_Route_Information.csv"):
    route_df = pd.read_csv("5.Time_in_Route_Information.csv", encoding='utf-8', engine='python')
    print(f"   Source file has {len(route_df)} loads with ACTUAL time in route data")
    
    # Create lookup for actual time in route data
    actual_total_hour_route = {}
    actual_days_deviation = {}
    
    for _, row in route_df.iterrows():
        load_name = str(row.get('Load', '')).strip()
        total_hour = str(row.get('Time in Route (min)', '')).strip()
        days_dev = str(row.get('Time In Route Difference ( DJ - Planned)', '')).strip()
        
        if load_name != '' and load_name != 'nan':
            if total_hour != '' and total_hour != 'nan':
                actual_total_hour_route[load_name] = total_hour
            if days_dev != '' and days_dev != 'nan':
                actual_days_deviation[load_name] = days_dev
    
    print(f"   ✅ Found actual total hour route for {len(actual_total_hour_route)} loads")
    print(f"   ✅ Found actual days deviation for {len(actual_days_deviation)} loads")
    
    # Apply ONLY actual time in route data
    hour_route_filled = 0
    days_dev_filled = 0
    
    for idx, row in final_df.iterrows():
        load_number = str(row['Load Number']).strip()
        
        # Fill time in route data with actual data only
        if load_number in actual_total_hour_route:
            final_df.at[idx, 'Total Hour Route'] = actual_total_hour_route[load_number]
            hour_route_filled += 1
        
        if load_number in actual_days_deviation:
            final_df.at[idx, 'Days In Route Deviation'] = actual_days_deviation[load_number]
            days_dev_filled += 1
    
    print(f"   ✅ Applied {hour_route_filled} ACTUAL total hour route times")
    print(f"   ✅ Applied {days_dev_filled} ACTUAL days deviation values")

# Save the file with ONLY actual data
output_filename = "Final_Consolidated_Data_ACTUAL_ONLY.csv"
excel_filename = "Final_Consolidated_Data_ACTUAL_ONLY.xlsx"

final_df.to_csv(output_filename, index=False)
final_df.to_excel(excel_filename, index=False)

# Final statistics - only show completion for fields that have actual data
print(f"\n📊 FINAL COMPLETION STATS (ACTUAL DATA ONLY):")
total_records = len(final_df)

# Check completion for each field
planned_dep_filled = len(final_df[final_df['Planned Departure Time'].notna() & (final_df['Planned Departure Time'] != '')])
dj_dep_filled = len(final_df[final_df['Dj Departure Time'].notna() & (final_df['Dj Departure Time'] != '')])
clockin_filled = len(final_df[final_df['Clockin Time'].notna() & (final_df['Clockin Time'] != '')])
arrival_depot_filled = len(final_df[final_df['Arrival At Depot'].notna() & (final_df['Arrival At Depot'] != '')])
arrival_cust_filled = len(final_df[final_df['Arrival At Customer'].notna() & (final_df['Arrival At Customer'] != '')])
departure_cust_filled = len(final_df[final_df['Departure Time From Customer'].notna() & (final_df['Departure Time From Customer'] != '')])
service_time_filled = len(final_df[final_df['Service Time At Customer'].notna() & (final_df['Service Time At Customer'] != '')])
total_hour_filled = len(final_df[final_df['Total Hour Route'].notna() & (final_df['Total Hour Route'] != '')])

print(f"🚀 Planned Departure Time: {planned_dep_filled}/{total_records} ({(planned_dep_filled/total_records*100):.1f}%)")
print(f"🚀 DJ Departure Time: {dj_dep_filled}/{total_records} ({(dj_dep_filled/total_records*100):.1f}%)")
print(f"🕐 Clockin Time: {clockin_filled}/{total_records} ({(clockin_filled/total_records*100):.1f}%)")
print(f"🏢 Arrival At Depot: {arrival_depot_filled}/{total_records} ({(arrival_depot_filled/total_records*100):.1f}%)")
print(f"🏢 Arrival At Customer: {arrival_cust_filled}/{total_records} ({(arrival_cust_filled/total_records*100):.1f}%)")
print(f"🏢 Departure From Customer: {departure_cust_filled}/{total_records} ({(departure_cust_filled/total_records*100):.1f}%)")
print(f"🏢 Service Time At Customer: {service_time_filled}/{total_records} ({(service_time_filled/total_records*100):.1f}%)")
print(f"🚛 Total Hour Route: {total_hour_filled}/{total_records} ({(total_hour_filled/total_records*100):.1f}%)")

print(f"\n✅ FILES SAVED:")
print(f"   📄 CSV: {output_filename}")
print(f"   📊 Excel: {excel_filename}")

print(f"\n🎯 SUCCESS: Used ONLY actual data from your source files - no predictions!")
print("   Empty cells mean no actual data exists in source files for those loads.")
