import pandas as pd
import os
import numpy as np
import statistics
from datetime import datetime

# --- Configuration & Constants ---
# Wrappers to allow dynamic paths
def get_file_paths(base_dir='.'):
    return {
        "Depot": os.path.join(base_dir, "1.Depot_Departures.csv"),
        "Customer": os.path.join(base_dir, "2.Customer_Timestamps.csv"),
        "Distance": os.path.join(base_dir, "3.Distance_Information.csv"),
        "Timestamps": os.path.join(base_dir, "4.Timestamps_and_Duration.csv"),
        "TimeRoute": os.path.join(base_dir, "5.Time_in_Route_Information.csv")
    }

COLUMNS = [
    "Create Date", "Month Name", "Transporter", "Load Number", "Mode Of Capture", "Driver Name",
    "Vehicle Reg", "Customer Name", "Vol Hl", "Invoice Number", "Mwarehouse", "Budgeted Kms",
    "PlannedDistanceToCustomer", "Actual Km", "Km Deviation", "Comment", "Clockin Time",
    "Planned Departure Time", "Dj Departure Time", "Departure Deviation Min", "Ave Departure",
    "Comment Ave Departure", "Arrival At Customer", "Departure Time From Customer",
    "Service Time At Customer", "Comment Tat", "Arrival At Depot", "Clock Out", "Ave Arrival Time",
    "Comment Ave Arrival Time", "Actual Days In Route", "Bud Days In Route", "Days In Route Deviation",
    "Total Hour Route", "Driver Rest Hours In Route", "Total Wh", "Tlp", "D1", "D2", "D3", "D4",
    "Comment Ave Tir"
]

# --- Helper Functions ---

def extract_month_name(date_str):
    if pd.isna(date_str) or date_str == "":
        return ""
    try:
        if isinstance(date_str, str):
            if '/' in date_str:
                date_part = date_str.split(' ')[0]
                date_obj = pd.to_datetime(date_part, format='%d/%m/%Y', errors='coerce')
            else:
                date_obj = pd.to_datetime(date_str, errors='coerce')
        else:
            date_obj = pd.to_datetime(date_str, errors='coerce')
        
        if pd.notna(date_obj):
            return date_obj.strftime('%B')
    except:
        pass
    return ""

def load_csv(file_path, sep=None):
    if not os.path.exists(file_path):
        # Silent fail or log?
        return None
    try:
        # Try default comma
        df = pd.read_csv(file_path, encoding='utf-8', engine='python', sep=sep if sep else ',')
        # If one column and no sep specified, try tab
        if sep is None and len(df.columns) <= 1:
            df = pd.read_csv(file_path, sep='\t', encoding='utf-8', engine='python')
        return df
    except Exception as e:
        # Fallback to tab
        try:
             df = pd.read_csv(file_path, sep='\t', encoding='utf-8', engine='python')
             return df
        except Exception as e2:
            print(f"❌ Error reading {file_path}: {e}")
            return None

# --- Lookup Builders ---

def build_lookups(input_files):
    print("🔄 Building data lookups for cross-referencing...")
    
    # 1. Customer Lookup
    customer_lookup = {}
    df_cust = load_csv(input_files["Customer"])
    if df_cust is not None:
        for _, row in df_cust.iterrows():
            if pd.notna(row.get('load_name')) and pd.notna(row.get('customer_name')):
                customer_lookup[str(row['load_name']).strip()] = str(row['customer_name']).strip()
    
    # 2. Distance Lookup
    distance_lookup = {}
    df_dist = load_csv(input_files["Distance"])
    if df_dist is not None:
        for _, row in df_dist.iterrows():
            load_name = str(row.get('Load Name', '')).strip()
            if load_name:
                distance_lookup[load_name] = {
                    'PlannedDistanceToCustomer': str(row.get('PlannedDistanceToCustomer', '')).strip(),
                    'Budgeted Kms': str(row.get('Planned Load Distance', '')).strip(),
                    'Actual Km': str(row.get('Total DJ Distance for Load', '')).strip(),
                    'Km Deviation': str(row.get('Distance Difference (Planned vs DJ)', '')).strip(),
                    'customer': str(row.get('Customer', '')).strip(),
                    'Vehicle Reg': str(row.get('Vehicle Reg', '')).strip(),
                    'Driver Name': str(row.get('Driver Name', '')).strip(),
                    'Transporter': str(row.get('Hired/Own', '')).strip() 
                }

    # 3. Driver & Vehicle Lookups
    driver_lookup = {}
    driver_vehicle_lookup = {}
    vehicle_reg_lookup = {}
    transporter_lookup = {}
    
    # helper to process driver data
    def process_driver_source(df, load_col, driver_col, vehicle_col, transporter_col=None):
        if df is None: return
        for _, row in df.iterrows():
            load = str(row.get(load_col, '')).strip()
            driver = str(row.get(driver_col, '')).strip()
            vehicle = str(row.get(vehicle_col, '')).strip() if vehicle_col else ""
            transporter = str(row.get(transporter_col, '')).strip() if transporter_col else ""

            if load and driver:
                driver_lookup[load] = driver
            
            if driver and vehicle:
                d_key = driver.upper()
                if d_key not in driver_vehicle_lookup: driver_vehicle_lookup[d_key] = {}
                driver_vehicle_lookup[d_key][vehicle] = driver_vehicle_lookup[d_key].get(vehicle, 0) + 1
            
            if load and vehicle:
                vehicle_reg_lookup[load] = vehicle
            
            if load and transporter:
                transporter_lookup[load] = transporter

    # Process all files for driver/vehicle info
    process_driver_source(load_csv(input_files["Depot"]), "Load Name", "Driver Name", "Vehicle Reg", "Hired/Own")
    process_driver_source(load_csv(input_files["Customer"]), "load_name", "DriverName", None)
    process_driver_source(load_csv(input_files["Distance"]), "Load Name", "Driver Name", "Vehicle Reg", "Hired/Own")
    process_driver_source(load_csv(input_files["TimeRoute"]), "Load", "Driver", None)

    # 4. Clockin Lookup
    clockin_lookup = {}
    df_time = load_csv(input_files["Timestamps"])
    if df_time is not None:
        for _, row in df_time.iterrows():
            load = str(row.get('load_name', '')).strip()
            if load:
                clockin_lookup[load] = {
                    'Clockin Time': str(row.get('Load StartTime (Pre-Trip Start)', '')).strip(),
                    'Arrival At Depot': str(row.get('ArriveAtDepot(Odo)', '')).strip(),
                    'schedule_date': str(row.get('schedule_date', '')).strip()
                }

    return {
        'customer': customer_lookup,
        'distance': distance_lookup,
        'driver': driver_lookup,
        'driver_vehicle': driver_vehicle_lookup,
        'vehicle': vehicle_reg_lookup,
        'clockin': clockin_lookup,
        'transporter': transporter_lookup
    }

# --- Smart Fill Functions ---

def get_smart_value(load_number, primary_lookup, secondary_lookup=None, key=None):
    load_str = str(load_number).strip()
    if not load_str: return ""
    
    # Direct match
    if key:
        if load_str in primary_lookup:
            return primary_lookup[load_str].get(key, "")
    else:
        if load_str in primary_lookup:
            return primary_lookup[load_str]
    
    return ""

def fill_distance_data(row, lookups):
    load_num = str(row.get('Load Number', '')).strip()
    cust_name = str(row.get('Customer Name', '')).strip()
    
    # Try direct lookup
    data = lookups['distance'].get(load_num)
    
    # Fallback to customer median
    if not data and cust_name:
        # Find all data for this customer
        cust_vals = [d for d in lookups['distance'].values() 
                     if str(d.get('customer', '')).lower() == cust_name.lower()]
        
        if cust_vals:
            # Create a synthetic data object with medians
            data = {}
            for field in ['PlannedDistanceToCustomer', 'Budgeted Kms', 'Actual Km', 'Km Deviation']:
                try:
                    vals = [float(d[field]) for d in cust_vals if d.get(field)]
                    if vals:
                        data[field] = round(statistics.median(vals), 1)
                    else:
                        data[field] = ""
                except:
                     data[field] = ""
    
    if data:
        for field in ['PlannedDistanceToCustomer', 'Budgeted Kms', 'Actual Km', 'Km Deviation']:
            if pd.isna(row.get(field)) or str(row.get(field)).strip() == "":
                row[field] = data.get(field, "")
    return row

def fill_vehicle_reg(row, lookups):
    if pd.notna(row.get('Vehicle Reg')) and str(row.get('Vehicle Reg')).strip() != "":
        return row
    
    load_num = str(row.get('Load Number', '')).strip()
    driver_name = str(row.get('Driver Name', '')).strip().upper()
    
    # 1. Look up by Load
    if load_num in lookups['vehicle']:
        row['Vehicle Reg'] = lookups['vehicle'][load_num]
        return row
        
    # 2. Look up by Driver (most common vehicle)
    if driver_name in lookups['driver_vehicle']:
        v_counts = lookups['driver_vehicle'][driver_name]
        try:
            best_vehicle = max(v_counts.items(), key=lambda x: x[1])[0]
            row['Vehicle Reg'] = best_vehicle
            return row
        except: pass
        
    return row

def fill_clockin_data(row, lookups):
    # Only fill if missing
    needs_clockin = pd.isna(row.get('Clockin Time')) or str(row.get('Clockin Time')).strip() == ""
    needs_arr = pd.isna(row.get('Arrival At Depot')) or str(row.get('Arrival At Depot')).strip() == ""
    
    if not (needs_clockin or needs_arr):
        return row

    load_num = str(row.get('Load Number', '')).strip()
    data = lookups['clockin'].get(load_num)
    
    if data:
        if needs_clockin: row['Clockin Time'] = data.get('Clockin Time', "")
        if needs_arr: row['Arrival At Depot'] = data.get('Arrival At Depot', "")
    
    return row

# --- Processing Functions ---

def process_file(filename, map_func, lookups):
    df = load_csv(filename)
    if df is None: return pd.DataFrame() # Return empty if file missing
    
    processed_rows = []
    print(f"Processing {filename}...")
    
    for _, row in df.iterrows():
        new_row = map_func(row, lookups)
        # Apply strict column filtering
        final_row = {k: new_row.get(k, "") for k in COLUMNS}
        processed_rows.append(final_row)
        
    return pd.DataFrame(processed_rows)

def map_depot(row, lookups):
    r = {}
    r["Create Date"] = row.get("Schedule Date")
    r["Month Name"] = extract_month_name(r["Create Date"])
    r["Load Number"] = row.get("Load Name")
    r["Driver Name"] = row.get("Driver Name")
    r["Vehicle Reg"] = row.get("Vehicle Reg")
    r["Planned Departure Time"] = row.get("Planned Departure Time")
    r["Dj Departure Time"] = row.get("DJ Departure Time")
    r["Departure Deviation Min"] = row.get("Departure Time Difference (DJ vs Planned)")
    r["Transporter"] = row.get("Hired/Own")
    r["Mwarehouse"] = "jinja"
    r["Mode Of Capture"] = "DJ"
    
    # Logic to fill missing
    r["Customer Name"] = get_smart_value(r["Load Number"], lookups['customer'])
    if not r["Driver Name"]: r["Driver Name"] = get_smart_value(r["Load Number"], lookups['driver'])
    
    return r

def map_customer(row, lookups):
    r = {}
    r["Create Date"] = row.get("schedule_date")
    r["Month Name"] = extract_month_name(r["Create Date"])
    r["Load Number"] = row.get("load_name")
    r["Driver Name"] = row.get("DriverName")
    r["Customer Name"] = row.get("customer_name")
    r["Invoice Number"] = row.get("sales_order_number")
    r["Arrival At Customer"] = row.get("ArrivedAtCustomer(Odo)")
    r["Departure Time From Customer"] = row.get("Offloading")
    r["Service Time At Customer"] = row.get("Total Time Spent @ Customer")
    r["Mwarehouse"] = "jinja"
    r["Mode Of Capture"] = "DJ"
    
    if not r["Driver Name"]: r["Driver Name"] = get_smart_value(r["Load Number"], lookups['driver'])
    return r

def map_distance(row, lookups):
    r = {}
    r["Create Date"] = row.get("Schedule Date")
    r["Month Name"] = extract_month_name(r["Create Date"])
    r["Load Number"] = row.get("Load Name")
    r["Driver Name"] = row.get("Driver Name")
    r["Vehicle Reg"] = row.get("Vehicle Reg")
    r["Customer Name"] = row.get("Customer")
    r["Invoice Number"] = row.get("Sales Order")
    r["PlannedDistanceToCustomer"] = row.get("PlannedDistanceToCustomer")
    r["Actual Km"] = row.get("Total DJ Distance for Load")
    r["Km Deviation"] = row.get("Distance Difference (Planned vs DJ)")
    r["Transporter"] = row.get("Hired/Own")
    r["Mwarehouse"] = "jinja"
    r["Mode Of Capture"] = "DJ"
    
    if not r["Customer Name"]: r["Customer Name"] = get_smart_value(r["Load Number"], lookups['customer'])
    if not r["Driver Name"]: r["Driver Name"] = get_smart_value(r["Load Number"], lookups['driver'])
    return r

def map_timestamps(row, lookups):
    r = {}
    r["Create Date"] = row.get("schedule_date")
    r["Month Name"] = extract_month_name(r["Create Date"])
    r["Load Number"] = row.get("load_name")
    r["Clockin Time"] = row.get("Load StartTime (Pre-Trip Start)")
    r["Arrival At Depot"] = row.get("ArriveAtDepot(Odo)")
    r["Mwarehouse"] = "jinja"
    r["Mode Of Capture"] = "DJ"
    
    r["Customer Name"] = get_smart_value(r["Load Number"], lookups['customer'])
    r["Driver Name"] = get_smart_value(r["Load Number"], lookups['driver'])
    return r

def map_timeroute(row, lookups):
    r = {}
    r["Create Date"] = row.get("Schedule Date")
    r["Month Name"] = extract_month_name(r["Create Date"])
    r["Load Number"] = row.get("Load")
    r["Driver Name"] = row.get("Driver")
    r["Customer Name"] = row.get("Customer")
    r["Invoice Number"] = row.get("Sales Order")
    r["Total Hour Route"] = row.get("Time in Route (min)")
    r["Days In Route Deviation"] = row.get("Time In Route Difference ( DJ - Planned)")
    r["Mwarehouse"] = "jinja"
    r["Mode Of Capture"] = "DJ"
    
    if not r["Customer Name"]: r["Customer Name"] = get_smart_value(r["Load Number"], lookups['customer'])
    if not r["Driver Name"]: r["Driver Name"] = get_smart_value(r["Load Number"], lookups['driver'])
    return r

# --- Calculation Logic (Legacy support) ---

def perform_final_calcs(df):
    print("🧮 Performing final route calculations...")
    
    def calc_days(row, start_col, end_col):
        try:
            dep = pd.to_datetime(row.get(start_col))
            arr = pd.to_datetime(row.get(end_col))
            if pd.notna(dep) and pd.notna(arr):
                val = (arr - dep).total_seconds() / (24 * 3600)
                return round(val, 2)
        except: pass
        return np.nan

    def calc_hours(row, start_col, end_col):
        try:
            dep = pd.to_datetime(row.get(start_col))
            arr = pd.to_datetime(row.get(end_col))
            if pd.notna(dep) and pd.notna(arr):
                val = (arr - dep).total_seconds() / 3600
                return round(val, 2)
        except: pass
        return np.nan

    for idx, row in df.iterrows():
        # Actual Days
        if pd.isna(row.get('Actual Days In Route')) or row.get('Actual Days In Route') == "":
            df.at[idx, 'Actual Days In Route'] = calc_days(row, 'Dj Departure Time', 'Arrival At Depot')
            
        # Bud Days
        if pd.isna(row.get('Bud Days In Route')) or row.get('Bud Days In Route') == "":
            df.at[idx, 'Bud Days In Route'] = calc_days(row, 'Planned Departure Time', 'Arrival At Depot')
            
        # Total Hours
        if pd.isna(row.get('Total Hour Route')) or row.get('Total Hour Route') == "":
            df.at[idx, 'Total Hour Route'] = calc_hours(row, 'Dj Departure Time', 'Arrival At Depot')
            
    # Deviations
    df['Actual Days In Route'] = pd.to_numeric(df['Actual Days In Route'], errors='coerce')
    df['Bud Days In Route'] = pd.to_numeric(df['Bud Days In Route'], errors='coerce')
    
    mask = df['Days In Route Deviation'].isna() | (df['Days In Route Deviation'] == "")
    if mask.any():
        df.loc[mask, 'Days In Route Deviation'] = df.loc[mask, 'Actual Days In Route'] - df.loc[mask, 'Bud Days In Route']

    return df

# --- Main Execution ---

def process_data_func(base_dir='.'):
    print(f"🚀 Starting Data Combiner App in {base_dir}...")
    
    input_files = get_file_paths(base_dir)
    
    # 1. Build Lookups
    lookups = build_lookups(input_files)
    
    # 2. Process Files
    dfs = []
    if os.path.exists(input_files["Depot"]): dfs.append(process_file(input_files["Depot"], map_depot, lookups))
    if os.path.exists(input_files["Customer"]): dfs.append(process_file(input_files["Customer"], map_customer, lookups))
    if os.path.exists(input_files["Distance"]): dfs.append(process_file(input_files["Distance"], map_distance, lookups))
    if os.path.exists(input_files["Timestamps"]): dfs.append(process_file(input_files["Timestamps"], map_timestamps, lookups))
    if os.path.exists(input_files["TimeRoute"]): dfs.append(process_file(input_files["TimeRoute"], map_timeroute, lookups))
    
    if not dfs:
        return "❌ No data processed! Please ensure CSV files are present."

    # 3. Concatenate
    print("🔗 Combining dataframes...")
    final_df = pd.concat(dfs, ignore_index=True)
    
    # 4. Consolidate by Load Number
    print("🧹 Consolidating duplicate load numbers...")
    
    def first_valid(series):
        v = series.dropna()
        v = v[v != ""]
        return v.iloc[0] if not v.empty else ""

    final_df['Load Number'] = final_df['Load Number'].astype(str).str.strip()
    final_df = final_df[final_df['Load Number'] != ""]
    final_df = final_df[final_df['Load Number'] != "nan"]
    
    final_df = final_df.groupby('Load Number', as_index=False).agg(first_valid)
    
    # 5. Post-Consolidation Filling
    print("🧠 Applying smart fill logic to consolidated data...")
    final_df = final_df.apply(lambda row: fill_distance_data(row, lookups), axis=1)
    final_df = final_df.apply(lambda row: fill_vehicle_reg(row, lookups), axis=1)
    final_df = final_df.apply(lambda row: fill_clockin_data(row, lookups), axis=1)
    
    for idx, row in final_df.iterrows():
        if not row['Transporter']:
            final_df.at[idx, 'Transporter'] = lookups['transporter'].get(row['Load Number'], "")
            
    # 6. Final Calculations
    final_df = perform_final_calcs(final_df)
    
    # 7. Sorting
    month_order = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4,
        'May': 5, 'June': 6, 'July': 7, 'August': 8,
        'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    final_df['Month_Idx'] = final_df['Month Name'].map(month_order).fillna(99)
    final_df = final_df.sort_values('Month_Idx')
    final_df = final_df.drop('Month_Idx', axis=1)
    
    # 8. Save
    print(f"💾 Saving {len(final_df)} records to Excel/CSV...")
    output_excel = os.path.join(base_dir, "Final_Consolidated_Data_Complete.xlsx")
    output_csv = os.path.join(base_dir, "Final_Consolidated_Data_Complete.csv")
    
    final_df.to_excel(output_excel, index=False)
    final_df.to_csv(output_csv, index=False)
    return "✅ Done! Files saved successfully."

if __name__ == "__main__":
    process_data_func()
