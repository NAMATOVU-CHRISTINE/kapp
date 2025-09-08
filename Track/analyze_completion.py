import pandas as pd

print("🔍 ANALYZING DISTANCE DATA COMPLETION")
print("=" * 45)

# Read files
df = pd.read_csv('Final_Consolidated_Data_Updated_temp.csv')
distance_df = pd.read_csv('3.Distance_Information.csv')

print(f"Consolidated data: {len(df)} records")
print(f"Distance Information: {len(distance_df)} records")

# Check completion
filled_budgeted = df[df['Budgeted Kms'].notna() & (df['Budgeted Kms'] != '')].shape[0]
print(f"Current Budgeted Kms completion: {filled_budgeted}/{len(df)} ({filled_budgeted/len(df)*100:.1f}%)")

# Check load matching
consolidated_loads = set(df['Load Number'].astype(str))
distance_loads = set(distance_df['Load Name'].astype(str))
exact_matches = consolidated_loads.intersection(distance_loads)
missing_loads = consolidated_loads - distance_loads

print(f"\nLoad matching analysis:")
print(f"  Exact matches: {len(exact_matches)}")
print(f"  Missing from Distance Info: {len(missing_loads)}")

# Sample missing loads
print(f"\nSample loads missing from Distance Info:")
for load in list(missing_loads)[:10]:
    print(f"  - {load}")

# Show available Distance Info loads
print(f"\nSample loads available in Distance Info:")
for load in list(distance_loads)[:10]:
    print(f"  - {load}")

# Check if there are unmatched distance records we could use
print(f"\nDistance Info loads not used:")
unused_distance = distance_loads - consolidated_loads
print(f"  Unused loads in Distance Info: {len(unused_distance)}")

print(f"\n🎯 TO ACHIEVE 100%:")
print(f"✅ We can fill {len(exact_matches)} records with exact matches")
print(f"❓ We need strategy for {len(missing_loads)} records without source data")
