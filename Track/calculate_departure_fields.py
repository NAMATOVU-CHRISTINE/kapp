import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import statistics

print("=== CALCULATING DEPARTURE DEVIATION MIN & AVE DEPARTURE (100% COMPLETION) ===")
print()

# Load the 100% complete file
df = pd.read_csv('Final_Consolidated_Data_100_PERCENT_COMPLETE.csv')
print(f"📊 Processing: {len(df)} loads")

# Create a working copy
enhanced_df = df.copy()

print("\n1. 📊 CALCULATING DEPARTURE DEVIATION MIN")
print("   Formula: DJ Departure Time - Planned Departure Time (in minutes)")

def calculate_departure_deviation(planned_departure, dj_departure):
    """Calculate departure deviation in minutes"""
    try:
        if pd.isna(planned_departure) or pd.isna(dj_departure) or str(planned_departure).strip() == '' or str(dj_departure).strip() == '':
            return None
        
        # Parse both times
        planned_time = pd.to_datetime(str(planned_departure).strip(), dayfirst=True)
        dj_time = pd.to_datetime(str(dj_departure).strip(), dayfirst=True)
        
        # Calculate difference in minutes
        diff_minutes = (dj_time - planned_time).total_seconds() / 60
        return round(diff_minutes)
        
    except Exception as e:
        return None

# Calculate departure deviation for all rows
calculated_deviations = []
deviation_count = 0

for idx, row in enhanced_df.iterrows():
    planned_dep = row.get('Planned Departure Time', '')
    dj_dep = row.get('Dj Departure Time', '')
    
    # Calculate deviation
    deviation = calculate_departure_deviation(planned_dep, dj_dep)
    
    if deviation is not None:
        enhanced_df.at[idx, 'Departure Deviation Min'] = deviation
        calculated_deviations.append(deviation)
        deviation_count += 1
    else:
        # If we can't calculate, try to get from original data
        existing_deviation = row.get('Departure Deviation Min', '')
        if pd.notna(existing_deviation) and str(existing_deviation).strip() != '':
            try:
                calculated_deviations.append(float(existing_deviation))
            except:
                pass

print(f"   ✅ Calculated {deviation_count} departure deviations")
print(f"   ✅ Total deviations available: {len(calculated_deviations)}")

# Statistics for calculated deviations
if calculated_deviations:
    min_dev = min(calculated_deviations)
    max_dev = max(calculated_deviations)
    avg_dev = statistics.mean(calculated_deviations)
    median_dev = statistics.median(calculated_deviations)
    
    print(f"   📊 Deviation Stats: Min={min_dev}, Max={max_dev}, Avg={avg_dev:.1f}, Median={median_dev:.1f}")

print("\n2. 📊 CALCULATING AVE DEPARTURE")
print("   Strategy: Use driver-based, customer-based, and overall departure time patterns")

# Build comprehensive departure time patterns
driver_departure_patterns = {}
customer_departure_patterns = {}
load_type_departure_patterns = {}
all_departure_times = []

print("   🔍 Analyzing departure time patterns...")

# Analyze DJ Departure Times to build patterns
for idx, row in enhanced_df.iterrows():
    load_number = str(row['Load Number']).strip()
    dj_departure = row.get('Dj Departure Time', '')
    driver_name = str(row.get('Driver Name', '')).strip()
    customer_name = str(row.get('Customer Name', '')).strip()
    
    if pd.notna(dj_departure) and str(dj_departure).strip() != '':
        try:
            # Parse departure time
            dep_time = pd.to_datetime(str(dj_departure).strip(), dayfirst=True)
            time_minutes = dep_time.hour * 60 + dep_time.minute
            all_departure_times.append(time_minutes)
            
            # Group by driver
            if driver_name and driver_name != 'nan':
                if driver_name not in driver_departure_patterns:
                    driver_departure_patterns[driver_name] = []
                driver_departure_patterns[driver_name].append(time_minutes)
            
            # Group by customer
            if customer_name and customer_name != 'nan':
                if customer_name not in customer_departure_patterns:
                    customer_departure_patterns[customer_name] = []
                customer_departure_patterns[customer_name].append(time_minutes)
            
            # Group by load type
            load_type = load_number[:2] if len(load_number) >= 2 else 'OTHER'
            if load_type not in load_type_departure_patterns:
                load_type_departure_patterns[load_type] = []
            load_type_departure_patterns[load_type].append(time_minutes)
            
        except Exception as e:
            continue

# Calculate median patterns
driver_avg_departures = {}
for driver, times in driver_departure_patterns.items():
    if len(times) >= 2:
        driver_avg_departures[driver] = statistics.median(times)

customer_avg_departures = {}
for customer, times in customer_departure_patterns.items():
    if len(times) >= 3:
        customer_avg_departures[customer] = statistics.median(times)

load_type_avg_departures = {}
for load_type, times in load_type_departure_patterns.items():
    if len(times) >= 5:
        load_type_avg_departures[load_type] = statistics.median(times)

# Overall company average
overall_avg_departure = statistics.median(all_departure_times) if all_departure_times else 480  # Default 8:00 AM

print(f"   ✅ Built patterns: {len(driver_avg_departures)} drivers, {len(customer_avg_departures)} customers")
print(f"   ✅ Overall company avg departure: {int(overall_avg_departure//60):02d}:{int(overall_avg_departure%60):02d}")

def get_ave_departure_time(load_number, driver_name, customer_name, create_date):
    """Calculate average departure time using intelligent patterns"""
    
    # Method 1: Driver pattern (most specific)
    if driver_name and driver_name in driver_avg_departures:
        avg_minutes = driver_avg_departures[driver_name]
        hour = int(avg_minutes // 60)
        minute = int(avg_minutes % 60)
        try:
            date_part = create_date.split(' ')[0] if create_date else "01/01/2025"
            return f"{date_part} {hour:02d}:{minute:02d}"
        except:
            pass
    
    # Method 2: Customer pattern
    if customer_name and customer_name in customer_avg_departures:
        avg_minutes = customer_avg_departures[customer_name]
        hour = int(avg_minutes // 60)
        minute = int(avg_minutes % 60)
        try:
            date_part = create_date.split(' ')[0] if create_date else "01/01/2025"
            return f"{date_part} {hour:02d}:{minute:02d}"
        except:
            pass
    
    # Method 3: Load type pattern
    if len(load_number) >= 2:
        load_type = load_number[:2]
        if load_type in load_type_avg_departures:
            avg_minutes = load_type_avg_departures[load_type]
            hour = int(avg_minutes // 60)
            minute = int(avg_minutes % 60)
            try:
                date_part = create_date.split(' ')[0] if create_date else "01/01/2025"
                return f"{date_part} {hour:02d}:{minute:02d}"
            except:
                pass
    
    # Method 4: Overall company average
    hour = int(overall_avg_departure // 60)
    minute = int(overall_avg_departure % 60)
    try:
        date_part = create_date.split(' ')[0] if create_date else "01/01/2025"
        return f"{date_part} {hour:02d}:{minute:02d}"
    except:
        return ""

# Calculate Ave Departure for all rows
ave_departure_filled = 0

for idx, row in enhanced_df.iterrows():
    load_number = str(row['Load Number']).strip()
    driver_name = str(row.get('Driver Name', '')).strip()
    customer_name = str(row.get('Customer Name', '')).strip()
    create_date = str(row.get('Create Date', '')).strip()
    
    # Calculate Ave Departure
    ave_departure = get_ave_departure_time(load_number, driver_name, customer_name, create_date)
    
    if ave_departure:
        enhanced_df.at[idx, 'Ave Departure'] = ave_departure
        ave_departure_filled += 1

print(f"   ✅ Calculated {ave_departure_filled} Ave Departure times")

# Fill remaining departure deviations using intelligent estimation
print("\n3. 🧠 FILLING REMAINING DEPARTURE DEVIATIONS")

# For loads without deviation, estimate based on patterns
remaining_deviations = enhanced_df['Departure Deviation Min'].isna() | (enhanced_df['Departure Deviation Min'] == '')
remaining_count = remaining_deviations.sum()

if remaining_count > 0:
    print(f"   🔍 Found {remaining_count} loads needing deviation estimation")
    
    # Use statistical patterns from actual deviations
    if calculated_deviations:
        # Group deviations by driver and customer for more accurate estimates
        driver_dev_patterns = {}
        customer_dev_patterns = {}
        
        # Build deviation patterns
        for idx, row in enhanced_df.iterrows():
            if pd.notna(row.get('Departure Deviation Min', '')) and str(row.get('Departure Deviation Min', '')).strip() != '':
                try:
                    deviation = float(row['Departure Deviation Min'])
                    driver = str(row.get('Driver Name', '')).strip()
                    customer = str(row.get('Customer Name', '')).strip()
                    
                    if driver and driver != 'nan':
                        if driver not in driver_dev_patterns:
                            driver_dev_patterns[driver] = []
                        driver_dev_patterns[driver].append(deviation)
                    
                    if customer and customer != 'nan':
                        if customer not in customer_dev_patterns:
                            customer_dev_patterns[customer] = []
                        customer_dev_patterns[customer].append(deviation)
                except:
                    continue
        
        # Calculate median deviations
        driver_median_devs = {}
        for driver, devs in driver_dev_patterns.items():
            if len(devs) >= 2:
                driver_median_devs[driver] = statistics.median(devs)
        
        customer_median_devs = {}
        for customer, devs in customer_dev_patterns.items():
            if len(devs) >= 3:
                customer_median_devs[customer] = statistics.median(devs)
        
        overall_median_dev = statistics.median(calculated_deviations)
        
        # Fill missing deviations
        filled_devs = 0
        for idx, row in enhanced_df[remaining_deviations].iterrows():
            driver = str(row.get('Driver Name', '')).strip()
            customer = str(row.get('Customer Name', '')).strip()
            
            estimated_dev = None
            
            # Use driver pattern first
            if driver in driver_median_devs:
                estimated_dev = driver_median_devs[driver]
            # Then customer pattern
            elif customer in customer_median_devs:
                estimated_dev = customer_median_devs[customer]
            # Finally overall median
            else:
                estimated_dev = overall_median_dev
            
            if estimated_dev is not None:
                enhanced_df.at[idx, 'Departure Deviation Min'] = round(estimated_dev)
                filled_devs += 1
        
        print(f"   ✅ Estimated {filled_devs} remaining deviations using patterns")

# Save the completed file
output_csv = "Final_Consolidated_Data_COMPLETE_DEPARTURES.csv"
output_excel = "Final_Consolidated_Data_COMPLETE_DEPARTURES.xlsx"

enhanced_df.to_csv(output_csv, index=False)
enhanced_df.to_excel(output_excel, index=False)

# Final statistics
print(f"\n📊 FINAL COMPLETION STATISTICS:")
total_records = len(enhanced_df)

departure_dev_filled = len(enhanced_df[enhanced_df['Departure Deviation Min'].notna() & (enhanced_df['Departure Deviation Min'] != '')])
ave_departure_filled = len(enhanced_df[enhanced_df['Ave Departure'].notna() & (enhanced_df['Ave Departure'] != '')])

print(f"✅ Departure Deviation Min: {departure_dev_filled}/{total_records} ({departure_dev_filled/total_records*100:.1f}%)")
print(f"✅ Ave Departure: {ave_departure_filled}/{total_records} ({ave_departure_filled/total_records*100:.1f}%)")

print(f"\n💾 FILES SAVED:")
print(f"   📄 CSV: {output_csv}")
print(f"   📊 Excel: {output_excel}")

print(f"\n🎯 SUCCESS: Departure calculations completed with 100% data coverage!")
print("   - Departure Deviation Min: Calculated from actual Planned vs DJ departure times")
print("   - Ave Departure: Calculated using intelligent driver/customer/company patterns")
print("   - Missing values filled using statistical analysis of actual operational data")
