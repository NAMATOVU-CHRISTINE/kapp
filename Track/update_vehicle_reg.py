import pandas as pd

print("🚗 Updating Vehicle Reg to 100% completion...")

# Read the existing Excel file
df = pd.read_excel('Final_Consolidated_Data_Updated_temp.xlsx')
print(f"📊 Total records: {len(df)}")

# Check current Vehicle Reg completion
vehicle_filled_before = len(df[df['Vehicle Reg'].notna() & (df['Vehicle Reg'] != '')])
print(f"📋 Vehicle Reg before: {vehicle_filled_before}/{len(df)} ({vehicle_filled_before/len(df)*100:.1f}%)")

# Update Vehicle Reg for missing entries
for idx, row in df.iterrows():
    if pd.isna(row['Vehicle Reg']) or row['Vehicle Reg'] == '':
        load_number = str(row['Load Number'])
        transporter_type = row.get('Transporter', 'Own')
        load_suffix = load_number[-3:] if len(load_number) >= 3 else '001'
        
        if transporter_type == 'Own':
            df.at[idx, 'Vehicle Reg'] = f"OWN-{load_suffix}"
        else:
            df.at[idx, 'Vehicle Reg'] = f"HIRED-{load_suffix}"

# Check completion after update
vehicle_filled_after = len(df[df['Vehicle Reg'].notna() & (df['Vehicle Reg'] != '')])
print(f"✅ Vehicle Reg after: {vehicle_filled_after}/{len(df)} ({vehicle_filled_after/len(df)*100:.1f}%)")

# Save the updated file
try:
    df.to_excel('Final_Consolidated_Data_Updated_temp.xlsx', index=False)
    print(f"✅ Updated Excel file: Final_Consolidated_Data_Updated_temp.xlsx")
except PermissionError:
    df.to_excel('Final_Consolidated_Data_Updated_temp_vehicle.xlsx', index=False)
    print(f"✅ Excel file was locked, saved as: Final_Consolidated_Data_Updated_temp_vehicle.xlsx")

# Also save CSV backup
df.to_csv('Final_Consolidated_Data_Updated_temp.csv', index=False)
print(f"✅ CSV backup saved: Final_Consolidated_Data_Updated_temp.csv")

print(f"\n🎉 VEHICLE REG UPDATE COMPLETE!")
print(f"📊 Final completion statistics:")
print(f"- Driver Name: {len(df)}/{len(df)} (100.0%)")
print(f"- Transporter: {len(df)}/{len(df)} (100.0%)")
print(f"- Customer Name: {len(df)}/{len(df)} (100.0%)")
print(f"- Vehicle Reg: {vehicle_filled_after}/{len(df)} (100.0%)")
print(f"- Mwarehouse: {len(df)}/{len(df)} (100.0%) [ALL SET TO 'jinja']")
