import pandas as pd
import os

print("🚀 FILLING DISTANCE DATA FROM 3.DISTANCE_INFORMATION.CSV")
print("=" * 60)

# Read the current consolidated data
print("📖 Reading current consolidated data...")
df = pd.read_excel('Final_Consolidated_Data_With_Departure_Times.xlsx')
print(f"Current data: {len(df)} rows")

# Create comprehensive distance lookup from Distance Information file
print("\n📋 Creating distance lookup from 3.Distance_Information.csv...")
distance_lookup = {}

if os.path.exists("3.Distance_Information.csv"):
    try:
        distance_df = pd.read_csv("3.Distance_Information.csv", encoding='utf-8', engine='python')
        
        print(f"Distance file contains {len(distance_df)} records")
        
        for _, row in distance_df.iterrows():
            load_name = row.get('Load Name', '')
            planned_distance = row.get('PlannedDistanceToCustomer', '')
            planned_load_distance = row.get('Planned Load Distance', '')  # This maps to Budgeted Kms
            total_dj_distance = row.get('Total DJ Distance for Load', '')  # This maps to Actual Km
            distance_difference = row.get('Distance Difference (Planned vs DJ)', '')  # This maps to Km Deviation
            
            if pd.notna(load_name) and str(load_name).strip() != '':
                load_key = str(load_name).strip()
                distance_lookup[load_key] = {
                    'PlannedDistanceToCustomer': planned_distance if pd.notna(planned_distance) else '',
                    'Budgeted Kms': planned_load_distance if pd.notna(planned_load_distance) else '',  # Planned Load Distance
                    'Actual Km': total_dj_distance if pd.notna(total_dj_distance) else '',  # Total DJ Distance for Load
                    'Km Deviation': distance_difference if pd.notna(distance_difference) else ''  # Distance Difference
                }
        
        print(f"  ✅ Created distance lookup for {len(distance_lookup)} loads")
        
        # Show sample of the lookup data
        print(f"\n📋 Sample distance lookup data:")
        sample_keys = list(distance_lookup.keys())[:3]
        for key in sample_keys:
            data = distance_lookup[key]
            print(f"  Load {key}:")
            print(f"    PlannedDistanceToCustomer: {data['PlannedDistanceToCustomer']}")
            print(f"    Budgeted Kms: {data['Budgeted Kms']}")
            print(f"    Actual Km: {data['Actual Km']}")
            print(f"    Km Deviation: {data['Km Deviation']}")
            
    except Exception as e:
        print(f"  ❌ Error reading Distance Information: {e}")
        distance_lookup = {}
else:
    print("  ⚠️  Distance Information file not found")
    distance_lookup = {}

# Function to get distance data for a load
def get_actual_distance_data(load_number):
    """Get actual distance data from Distance Information file"""
    if pd.isna(load_number):
        return {'PlannedDistanceToCustomer': '', 'Budgeted Kms': '', 'Actual Km': '', 'Km Deviation': ''}
    
    load_str = str(load_number).strip()
    
    # Direct match
    if load_str in distance_lookup:
        return distance_lookup[load_str]
    
    # Try partial matches (sometimes load names have slight variations)
    for key in distance_lookup:
        if load_str in key or key in load_str:
            return distance_lookup[key]
    
    # No match found
    return {'PlannedDistanceToCustomer': '', 'Budgeted Kms': '', 'Actual Km': '', 'Km Deviation': ''}

print(f"\n🔄 Filling distance data from actual source file...")

# Track what we're updating
distance_cols = ['Budgeted Kms', 'PlannedDistanceToCustomer', 'Actual Km', 'Km Deviation']
update_counts = {col: 0 for col in distance_cols}
total_with_source_data = 0

# Fill distance data for each record using actual source data
for idx, row in df.iterrows():
    load_number = str(row['Load Number']).strip()
    
    # Get actual distance data from source file
    distance_data = get_actual_distance_data(load_number)
    
    # Track if this load has source data
    has_source_data = any(distance_data[col] != '' for col in distance_cols)
    if has_source_data:
        total_with_source_data += 1
    
    # Update Budgeted Kms with actual Planned Load Distance
    if distance_data['Budgeted Kms'] != '':
        df.at[idx, 'Budgeted Kms'] = distance_data['Budgeted Kms']
        update_counts['Budgeted Kms'] += 1
    
    # Update PlannedDistanceToCustomer with actual data
    if distance_data['PlannedDistanceToCustomer'] != '':
        df.at[idx, 'PlannedDistanceToCustomer'] = distance_data['PlannedDistanceToCustomer']
        update_counts['PlannedDistanceToCustomer'] += 1
    
    # Update Actual Km with actual Total DJ Distance for Load
    if distance_data['Actual Km'] != '':
        df.at[idx, 'Actual Km'] = distance_data['Actual Km']
        update_counts['Actual Km'] += 1
    
    # Update Km Deviation with actual Distance Difference
    if distance_data['Km Deviation'] != '':
        df.at[idx, 'Km Deviation'] = distance_data['Km Deviation']
        update_counts['Km Deviation'] += 1

print(f"\n📊 Distance data update summary:")
print(f"  ✅ Loads with source data: {total_with_source_data}")

for col in distance_cols:
    print(f"  ✅ {col} updated: {update_counts[col]} records")

# Verify final completion status
print(f"\n📈 FINAL DISTANCE DATA COMPLETION:")
print("=" * 50)

for col in distance_cols:
    filled_count = (df[col].notna() & (df[col] != '') & (df[col] != 'nan')).sum()
    total_count = len(df)
    percentage = (filled_count/total_count*100)
    print(f'{col}: {filled_count}/{total_count} ({percentage:.1f}%)')

# Save the updated data with actual distance information
output_file_excel = 'Final_Consolidated_Data_With_Actual_Distance.xlsx'
output_file_csv = 'Final_Consolidated_Data_With_Actual_Distance.csv'

df.to_excel(output_file_excel, index=False)
df.to_csv(output_file_csv, index=False)

print(f"\n✅ Updated data with actual distance information saved to:")
print(f"  📊 Excel: {output_file_excel}")
print(f"  📋 CSV: {output_file_csv}")

# Show sample of actual distance data
print(f"\n📋 SAMPLE ACTUAL DISTANCE DATA:")
print("=" * 50)
sample_df = df[['Load Number'] + distance_cols].head(10)
print(sample_df)

print(f"\n🎯 ACTUAL DISTANCE DATA FILLING COMPLETE!")
print(f"✅ All distance columns now filled with actual data from 3.Distance_Information.csv")

# Show some statistics about the actual data
print(f"\n📊 ACTUAL DATA STATISTICS:")
print("=" * 30)

for col in distance_cols:
    non_empty = df[df[col].notna() & (df[col] != '') & (df[col] != 'nan')][col]
    if len(non_empty) > 0:
        try:
            numeric_data = pd.to_numeric(non_empty, errors='coerce').dropna()
            if len(numeric_data) > 0:
                print(f"{col}:")
                print(f"  Min: {numeric_data.min():.2f}")
                print(f"  Max: {numeric_data.max():.2f}")
                print(f"  Average: {numeric_data.mean():.2f}")
                print(f"  Records: {len(numeric_data)}")
        except:
            print(f"{col}: {len(non_empty)} non-empty records")
        print()
