import pandas as pd

print("🔍 FIXING DISTANCE DATA WITH ONLY ACTUAL VALUES")
print("=" * 50)

# Read the files
df = pd.read_csv('Final_Consolidated_Data_With_Actual_Distance.csv')
distance_df = pd.read_csv('3.Distance_Information.csv')

print(f"Consolidated data: {len(df)} records")
print(f"Distance Information: {len(distance_df)} records")

# Get actual load names from Distance Information
actual_loads = set(distance_df['Load Name'].dropna().astype(str))
consolidated_loads = set(df['Load Number'].dropna().astype(str))

print(f"\n📋 Load matching analysis:")
print(f"  Distance file loads: {len(actual_loads)}")
print(f"  Consolidated loads: {len(consolidated_loads)}")
print(f"  Matching loads: {len(actual_loads.intersection(consolidated_loads))}")

# Show sample loads from Distance Information
print(f"\n📋 Sample actual loads in Distance Information:")
for load in list(actual_loads)[:10]:
    print(f"  - {load}")

# Check which consolidated loads have matches
matching_loads = actual_loads.intersection(consolidated_loads)
print(f"\n✅ Found {len(matching_loads)} loads with actual distance data")

if len(matching_loads) > 0:
    print(f"\nSample matching loads:")
    for load in list(matching_loads)[:5]:
        print(f"  - {load}")

# Create proper distance lookup with ONLY actual data
print(f"\n🔄 Creating corrected distance mapping...")
corrected_distance_lookup = {}

for _, row in distance_df.iterrows():
    load_name = str(row['Load Name']).strip()
    planned_distance = row['PlannedDistanceToCustomer']
    planned_load_distance = row['Planned Load Distance']
    total_dj_distance = row['Total DJ Distance for Load']
    distance_difference = row['Distance Difference (Planned vs DJ)']
    
    if load_name in consolidated_loads:
        corrected_distance_lookup[load_name] = {
            'PlannedDistanceToCustomer': planned_distance,
            'Budgeted Kms': planned_load_distance,
            'Actual Km': total_dj_distance,
            'Km Deviation': distance_difference
        }

print(f"  ✅ Created corrected lookup for {len(corrected_distance_lookup)} loads")

# Now update ONLY the records that have actual data
updated_count = 0
cleared_count = 0

distance_cols = ['Budgeted Kms', 'PlannedDistanceToCustomer', 'Actual Km', 'Km Deviation']

for idx, row in df.iterrows():
    load_number = str(row['Load Number']).strip()
    
    if load_number in corrected_distance_lookup:
        # Use actual data
        actual_data = corrected_distance_lookup[load_number]
        df.at[idx, 'Budgeted Kms'] = actual_data['Budgeted Kms']
        df.at[idx, 'PlannedDistanceToCustomer'] = actual_data['PlannedDistanceToCustomer']
        df.at[idx, 'Actual Km'] = actual_data['Actual Km']
        df.at[idx, 'Km Deviation'] = actual_data['Km Deviation']
        updated_count += 1
    else:
        # Clear the fallback values - leave empty for loads without actual data
        df.at[idx, 'Budgeted Kms'] = ''
        df.at[idx, 'PlannedDistanceToCustomer'] = ''
        df.at[idx, 'Actual Km'] = ''
        df.at[idx, 'Km Deviation'] = ''
        cleared_count += 1

print(f"\n📊 Update summary:")
print(f"  ✅ Updated with actual data: {updated_count} records")
print(f"  🚫 Cleared fallback data: {cleared_count} records")

# Show completion status
print(f"\n📈 FINAL DISTANCE DATA STATUS:")
print("=" * 40)

for col in distance_cols:
    filled_count = (df[col].notna() & (df[col] != '') & (df[col] != 'nan')).sum()
    total_count = len(df)
    percentage = (filled_count/total_count*100)
    print(f'{col}: {filled_count}/{total_count} ({percentage:.1f}%)')

# Save the corrected data
output_file_excel = 'Final_Consolidated_Data_ACTUAL_Distance_Only.xlsx'
output_file_csv = 'Final_Consolidated_Data_ACTUAL_Distance_Only.csv'

df.to_excel(output_file_excel, index=False)
df.to_csv(output_file_csv, index=False)

print(f"\n✅ Corrected data saved to:")
print(f"  📊 Excel: {output_file_excel}")
print(f"  📋 CSV: {output_file_csv}")

# Show sample of actual data
print(f"\n📋 SAMPLE RECORDS WITH ACTUAL DISTANCE DATA:")
print("=" * 50)
actual_data_records = df[df['Load Number'].isin(list(matching_loads))][['Load Number'] + distance_cols].head(5)
print(actual_data_records)

print(f"\n🎯 CORRECTED - ONLY ACTUAL DATA FROM SOURCE FILE!")
print(f"✅ {updated_count} records have real distance data from 3.Distance_Information.csv")
print(f"✅ {cleared_count} records without source data are left empty (no fallback values)")
