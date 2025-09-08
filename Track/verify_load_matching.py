import pandas as pd

print("🔍 VERIFYING LOAD NUMBER = LOAD NAME MATCHING")
print("=" * 50)

# Read both files
df = pd.read_csv('Final_Consolidated_Data_ACTUAL_Distance_Only.csv')
distance_df = pd.read_csv('3.Distance_Information.csv')

print(f"Consolidated data: {len(df)} records")
print(f"Distance Information: {len(distance_df)} records")

print(f"\n📋 Sample Load Numbers from consolidated data:")
sample_loads_consolidated = df['Load Number'].head(10).tolist()
for load in sample_loads_consolidated:
    print(f"  - {load}")

print(f"\n📋 Sample Load Names from Distance Information:")
sample_loads_distance = distance_df['Load Name'].head(10).tolist()
for load in sample_loads_distance:
    print(f"  - {load}")

print(f"\n🔍 Checking if consolidated Load Numbers exist in Distance Info Load Names:")
exact_matches = 0
for load in sample_loads_consolidated:
    matches = distance_df[distance_df['Load Name'] == load]
    if len(matches) > 0:
        print(f"  ✅ {load}: FOUND")
        exact_matches += 1
    else:
        print(f"  ❌ {load}: NOT FOUND")

print(f"\nTotal exact matches from sample: {exact_matches}/{len(sample_loads_consolidated)}")

# Check the full dataset
print(f"\n📊 FULL DATASET ANALYSIS:")
consolidated_loads = set(df['Load Number'].astype(str))
distance_loads = set(distance_df['Load Name'].astype(str))

total_matches = len(consolidated_loads.intersection(distance_loads))
print(f"  Consolidated unique loads: {len(consolidated_loads)}")
print(f"  Distance Info unique loads: {len(distance_loads)}")
print(f"  Total exact matches: {total_matches}")
print(f"  Match percentage: {(total_matches/len(consolidated_loads))*100:.1f}%")

# Show some actual matching examples
matching_loads = list(consolidated_loads.intersection(distance_loads))
if len(matching_loads) > 0:
    print(f"\n✅ Examples of loads that DO match:")
    for load in matching_loads[:10]:
        distance_row = distance_df[distance_df['Load Name'] == load].iloc[0]
        print(f"  {load}:")
        print(f"    PlannedDistanceToCustomer: {distance_row['PlannedDistanceToCustomer']}")
        print(f"    Planned Load Distance: {distance_row['Planned Load Distance']}")
        print(f"    Total DJ Distance: {distance_row['Total DJ Distance for Load']}")
        print(f"    Distance Difference: {distance_row['Distance Difference (Planned vs DJ)']}")
        break  # Just show one example

# Show loads that don't match
non_matching = list(consolidated_loads - distance_loads)
if len(non_matching) > 0:
    print(f"\n❌ Examples of loads that DON'T match:")
    for load in non_matching[:5]:
        print(f"  - {load}")

print(f"\n🎯 CONCLUSION:")
if total_matches > 0:
    print(f"✅ YES - Load Number = Load Name for {total_matches} records")
    print(f"📊 We can fill distance data for {total_matches} loads with actual values")
else:
    print(f"❌ NO MATCHES FOUND - Load Numbers don't match Load Names")
    print(f"🔍 Need to investigate the data structure")
