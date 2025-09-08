import pandas as pd

# Read both files
print("🔍 CHECKING ACTUAL DATA MATCHING")
print("=" * 40)

df = pd.read_csv('Final_Consolidated_Data_With_Actual_Distance.csv')
distance_df = pd.read_csv('3.Distance_Information.csv')

print(f"Consolidated data: {len(df)} records")
print(f"Distance Information: {len(distance_df)} records")

print(f"\n📋 Sample load numbers from consolidated data:")
sample_loads = df['Load Number'].head(10).tolist()
for load in sample_loads:
    print(f"  - {load}")

print(f"\n🔍 Checking these loads in Distance Information file...")
for load in sample_loads:
    matching = distance_df[distance_df['Load Name'] == load]
    if len(matching) > 0:
        row = matching.iloc[0]
        print(f"\n✅ Found {load}:")
        print(f"  PlannedDistanceToCustomer: {row['PlannedDistanceToCustomer']}")
        print(f"  Planned Load Distance: {row['Planned Load Distance']}")
        print(f"  Total DJ Distance for Load: {row['Total DJ Distance for Load']}")
        print(f"  Distance Difference: {row['Distance Difference (Planned vs DJ)']}")
    else:
        print(f"\n❌ NOT FOUND: {load}")

print(f"\n🔍 Current values in consolidated data vs actual:")
print("=" * 50)
for i, load in enumerate(sample_loads[:5]):
    current_row = df[df['Load Number'] == load].iloc[0]
    distance_row = distance_df[distance_df['Load Name'] == load]
    
    print(f"\nLoad: {load}")
    print(f"  Current Budgeted Kms: {current_row['Budgeted Kms']}")
    print(f"  Current PlannedDistanceToCustomer: {current_row['PlannedDistanceToCustomer']}")
    print(f"  Current Actual Km: {current_row['Actual Km']}")
    print(f"  Current Km Deviation: {current_row['Km Deviation']}")
    
    if len(distance_row) > 0:
        actual = distance_row.iloc[0]
        print(f"  ACTUAL Planned Load Distance: {actual['Planned Load Distance']}")
        print(f"  ACTUAL PlannedDistanceToCustomer: {actual['PlannedDistanceToCustomer']}")
        print(f"  ACTUAL Total DJ Distance: {actual['Total DJ Distance for Load']}")
        print(f"  ACTUAL Distance Difference: {actual['Distance Difference (Planned vs DJ)']}")
        print(f"  ❗ MISMATCH DETECTED!")
    else:
        print(f"  No distance data found for this load")
