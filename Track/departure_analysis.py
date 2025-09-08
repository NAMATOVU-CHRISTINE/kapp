import pandas as pd

# Read the current consolidated data
df = pd.read_csv('Final_Consolidated_Data_Updated_temp.csv')

departure_cols = ['Planned Departure Time', 'Dj Departure Time', 'Departure Deviation Min', 'Ave Departure', 'Comment Ave Departure']

print('📊 DEPARTURE TIME COLUMN ANALYSIS:')
print('=' * 50)

for col in departure_cols:
    filled_count = (df[col] != "").sum()
    total_count = len(df)
    percentage = (filled_count/total_count*100)
    print(f'{col}: {filled_count}/{total_count} ({percentage:.1f}% filled)')

print('\n📋 SAMPLE DEPARTURE DATA:')
print('=' * 50)
print(df[departure_cols].head(10))

print('\n🔍 NON-EMPTY DEPARTURE DATA EXAMPLES:')
print('=' * 50)
# Show examples where departure data exists
for col in departure_cols:
    non_empty = df[df[col] != ""][col].head(3)
    if len(non_empty) > 0:
        print(f'\n{col} examples:')
        for val in non_empty:
            print(f'  - {val}')
