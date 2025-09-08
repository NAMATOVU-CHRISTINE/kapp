import pandas as pd
import numpy as np

print("🔧 FIXING DISTANCE DATA - USING ONLY UNIQUE ACTUAL VALUES")
print("=" * 60)

# Read the files
print("📖 Reading source files...")
df = pd.read_csv('Final_Consolidated_Data_ACTUAL_Distance_Only.csv')
distance_df = pd.read_csv('3.Distance_Information.csv')

print(f"Consolidated data: {len(df)} records")
print(f"Distance Information: {len(distance_df)} records")

# Check current repeated values issue
distance_cols = ['Budgeted Kms', 'PlannedDistanceToCustomer', 'Actual Km', 'Km Deviation']
print(f"\n🔍 ANALYZING CURRENT DATA ISSUES:")
print("=" * 40)

# Count how many records have the repeated values
repeated_values_count = len(df[(df['Budgeted Kms'] == 352.7) & 
                                (df['PlannedDistanceToCustomer'] == 176.2) & 
                                (df['Actual Km'] == -1606.7) & 
                                (df['Km Deviation'] == 9381.8)])

print(f"Records with repeated values (352.7, 176.2, -1606.7, 9381.8): {repeated_values_count}")

# Create a clean distance lookup with ONLY unique actual values
print(f"\n🔄 Creating clean lookup with actual unique values...")
clean_distance_lookup = {}

for _, row in distance_df.iterrows():
    load_name = str(row['Load Name']).strip()
    planned_distance = row['PlannedDistanceToCustomer']
    planned_load_distance = row['Planned Load Distance']
    total_dj_distance = row['Total DJ Distance for Load']
    distance_difference = row['Distance Difference (Planned vs DJ)']
    
    # Only add if all values are valid and not NaN
    if (pd.notna(planned_distance) and pd.notna(planned_load_distance) and 
        pd.notna(total_dj_distance) and pd.notna(distance_difference)):
        
        clean_distance_lookup[load_name] = {
            'PlannedDistanceToCustomer': float(planned_distance),
            'Budgeted Kms': float(planned_load_distance),
            'Actual Km': float(total_dj_distance),
            'Km Deviation': float(distance_difference)
        }

print(f"  ✅ Created clean lookup for {len(clean_distance_lookup)} loads with unique actual values")

# Show sample of actual unique values
print(f"\n📋 SAMPLE OF ACTUAL UNIQUE VALUES FROM SOURCE:")
print("=" * 50)
sample_loads = list(clean_distance_lookup.keys())[:10]
for load in sample_loads:
    data = clean_distance_lookup[load]
    print(f"{load}:")
    print(f"  Budgeted Kms: {data['Budgeted Kms']}")
    print(f"  PlannedDistanceToCustomer: {data['PlannedDistanceToCustomer']}")
    print(f"  Actual Km: {data['Actual Km']}")
    print(f"  Km Deviation: {data['Km Deviation']}")
    print()

# Clear ALL distance data first, then fill only with actual matches
print(f"\n🧹 CLEARING ALL DISTANCE DATA TO START FRESH...")
for col in distance_cols:
    df[col] = np.nan

# Fill ONLY with actual data from Distance Information file
print(f"\n📝 FILLING WITH ACTUAL DATA ONLY...")
filled_count = 0
not_found_count = 0

for idx, row in df.iterrows():
    load_number = str(row['Load Number']).strip()
    
    if load_number in clean_distance_lookup:
        # Use actual unique data
        actual_data = clean_distance_lookup[load_number]
        df.at[idx, 'Budgeted Kms'] = actual_data['Budgeted Kms']
        df.at[idx, 'PlannedDistanceToCustomer'] = actual_data['PlannedDistanceToCustomer']
        df.at[idx, 'Actual Km'] = actual_data['Actual Km']
        df.at[idx, 'Km Deviation'] = actual_data['Km Deviation']
        filled_count += 1
    else:
        # Leave empty - no fallback values
        not_found_count += 1

print(f"  ✅ Filled with actual data: {filled_count} records")
print(f"  📝 Left empty (no source data): {not_found_count} records")

# Verify no repeated values exist
print(f"\n✅ VERIFICATION - NO REPEATED VALUES:")
print("=" * 40)
repeated_after_fix = len(df[(df['Budgeted Kms'] == 352.7) & 
                           (df['PlannedDistanceToCustomer'] == 176.2) & 
                           (df['Actual Km'] == -1606.7) & 
                           (df['Km Deviation'] == 9381.8)])

print(f"Records with repeated values after fix: {repeated_after_fix}")

# Show completion statistics
print(f"\n📊 FINAL COMPLETION STATISTICS:")
print("=" * 35)

for col in distance_cols:
    filled = df[col].notna().sum()
    total = len(df)
    percentage = (filled/total*100)
    print(f'{col}: {filled}/{total} ({percentage:.1f}%)')

# Save the corrected data
output_file_excel = 'Final_Consolidated_Data_CLEAN_Actual_Distance.xlsx'
output_file_csv = 'Final_Consolidated_Data_CLEAN_Actual_Distance.csv'

df.to_excel(output_file_excel, index=False)
df.to_csv(output_file_csv, index=False)

print(f"\n✅ Cleaned data saved to:")
print(f"  📊 Excel: {output_file_excel}")
print(f"  📋 CSV: {output_file_csv}")

# Show sample of cleaned data with unique values
print(f"\n📋 SAMPLE OF CLEANED DATA WITH UNIQUE ACTUAL VALUES:")
print("=" * 55)
actual_data_df = df[df['Budgeted Kms'].notna()][['Load Number'] + distance_cols].head(10)
print(actual_data_df)

# Show data uniqueness verification
print(f"\n🔍 DATA UNIQUENESS VERIFICATION:")
print("=" * 35)
for col in distance_cols:
    non_null_values = df[df[col].notna()][col]
    if len(non_null_values) > 0:
        unique_values = non_null_values.nunique()
        total_values = len(non_null_values)
        print(f"{col}: {unique_values} unique values out of {total_values} filled records")

print(f"\n🎯 SUCCESS!")
print(f"✅ All distance data now comes from actual unique values in 3.Distance_Information.csv")
print(f"✅ No repeated fallback values (352.7, 176.2, -1606.7, 9381.8)")
print(f"✅ {filled_count} records have genuine actual distance data")
print(f"✅ {not_found_count} records left empty (no artificial data)")
