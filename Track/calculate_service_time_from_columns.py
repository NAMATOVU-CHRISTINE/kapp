import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def calculate_service_time_from_columns():
    """
    Calculate Service Time At Customer using Arrival At Customer and Departure Time From Customer
    """
    print("=== CALCULATING SERVICE TIME FROM ARRIVAL/DEPARTURE COLUMNS ===")
    print("Strategy: Calculate time difference between Arrival At Customer and Departure Time From Customer")
    print()
    
    # Read the current data
    df = pd.read_csv('Final_Consolidated_Data_SERVICE_TIME_100_PERCENT_ACTUAL.csv')
    
    print(f"📊 Total records: {len(df)}")
    
    # Check current completion
    current_service_times = df['Service Time At Customer'].notna().sum()
    print(f"📊 Current Service Time records: {current_service_times}")
    
    # Check availability of arrival and departure columns
    arrival_available = df['Arrival At Customer'].notna().sum()
    departure_available = df['Departure Time From Customer'].notna().sum()
    
    print(f"📊 Arrival At Customer available: {arrival_available}")
    print(f"📊 Departure Time From Customer available: {departure_available}")
    
    # Find records where we can calculate service time
    can_calculate_mask = (
        df['Arrival At Customer'].notna() & 
        df['Departure Time From Customer'].notna() &
        (df['Arrival At Customer'] != '') &
        (df['Departure Time From Customer'] != '')
    )
    
    can_calculate_count = can_calculate_mask.sum()
    print(f"📊 Records where service time can be calculated: {can_calculate_count}")
    
    # Reset Service Time to recalculate all values from arrival/departure
    print("\n🔄 RECALCULATING ALL SERVICE TIMES FROM ARRIVAL/DEPARTURE...")
    
    # Create new service time column
    df['Service Time At Customer - Calculated'] = np.nan
    
    calculated_count = 0
    error_count = 0
    
    for idx, row in df.iterrows():
        arrival_str = str(row['Arrival At Customer']).strip()
        departure_str = str(row['Departure Time From Customer']).strip()
        
        if arrival_str and departure_str and arrival_str != 'nan' and departure_str != 'nan':
            try:
                # Parse the timestamps
                arrival_time = pd.to_datetime(arrival_str, dayfirst=True)
                departure_time = pd.to_datetime(departure_str, dayfirst=True)
                
                # Calculate time difference in minutes
                time_diff = departure_time - arrival_time
                service_minutes = time_diff.total_seconds() / 60
                
                # Validate the calculated time (should be positive and reasonable)
                if 0 <= service_minutes <= 10080:  # Max 7 days (10080 minutes)
                    df.at[idx, 'Service Time At Customer - Calculated'] = service_minutes
                    calculated_count += 1
                else:
                    # Handle edge cases where departure might be next day
                    if service_minutes < 0:
                        # Try adding a day to departure time
                        departure_time_next_day = departure_time + timedelta(days=1)
                        time_diff_corrected = departure_time_next_day - arrival_time
                        service_minutes_corrected = time_diff_corrected.total_seconds() / 60
                        
                        if 0 <= service_minutes_corrected <= 10080:
                            df.at[idx, 'Service Time At Customer - Calculated'] = service_minutes_corrected
                            calculated_count += 1
                        else:
                            error_count += 1
                    else:
                        error_count += 1
                        
            except Exception as e:
                error_count += 1
                continue
    
    print(f"   ✅ Successfully calculated: {calculated_count} service times")
    print(f"   ⚠️  Calculation errors: {error_count}")
    
    # Use calculated values or keep existing ones
    print("\n🔄 UPDATING SERVICE TIME COLUMN...")
    
    updated_count = 0
    kept_existing = 0
    
    for idx, row in df.iterrows():
        calculated_value = row['Service Time At Customer - Calculated']
        existing_value = row['Service Time At Customer']
        
        # Use calculated value if available, otherwise keep existing
        if pd.notna(calculated_value):
            df.at[idx, 'Service Time At Customer'] = calculated_value
            updated_count += 1
        elif pd.notna(existing_value):
            kept_existing += 1
    
    print(f"   ✅ Updated with calculated values: {updated_count}")
    print(f"   ✅ Kept existing values: {kept_existing}")
    
    # Drop the temporary calculation column
    df = df.drop('Service Time At Customer - Calculated', axis=1)
    
    # Final statistics
    final_filled = df['Service Time At Customer'].notna().sum()
    final_missing = len(df) - final_filled
    completion_rate = (final_filled / len(df)) * 100
    
    print(f"\n🎯 FINAL RESULTS:")
    print(f"   Total records: {len(df)}")
    print(f"   Records with service time: {final_filled}")
    print(f"   Records missing: {final_missing}")
    print(f"   Completion rate: {completion_rate:.1f}%")
    
    # Show service time statistics
    service_times = df['Service Time At Customer'].dropna()
    print(f"\n📈 SERVICE TIME STATISTICS:")
    print(f"   Minimum: {service_times.min():.1f} minutes ({service_times.min()/60:.1f} hours)")
    print(f"   Maximum: {service_times.max():.1f} minutes ({service_times.max()/60:.1f} hours)")
    print(f"   Median: {service_times.median():.1f} minutes ({service_times.median()/60:.1f} hours)")
    print(f"   Average: {service_times.mean():.1f} minutes ({service_times.mean()/60:.1f} hours)")
    
    # Show some examples of calculated service times
    print(f"\n📋 SAMPLE CALCULATED SERVICE TIMES:")
    sample_data = df[['Load Number', 'Arrival At Customer', 'Departure Time From Customer', 'Service Time At Customer']].head(10)
    
    for _, row in sample_data.iterrows():
        load_num = row['Load Number']
        arrival = row['Arrival At Customer']
        departure = row['Departure Time From Customer'] 
        service_time = row['Service Time At Customer']
        
        if pd.notna(service_time):
            print(f"   {load_num}: {arrival} → {departure} = {service_time:.0f} min")
    
    # Save the updated file
    output_csv = "Final_Consolidated_Data_SERVICE_TIME_CALCULATED.csv"
    output_excel = "Final_Consolidated_Data_SERVICE_TIME_CALCULATED.xlsx"
    
    df.to_csv(output_csv, index=False)
    df.to_excel(output_excel, index=False)
    
    print(f"\n💾 FILES SAVED:")
    print(f"   📄 CSV: {output_csv}")
    print(f"   📊 Excel: {output_excel}")
    
    print(f"\n🏆 SUCCESS: Service Time At Customer calculated from arrival/departure columns!")
    print("   ✅ Used actual arrival and departure timestamps")
    print("   ✅ Calculated precise time differences in minutes")
    print("   ✅ Handled edge cases (overnight stays, etc.)")
    print(f"   ✅ Achieved {completion_rate:.1f}% completion")
    
    return df

if __name__ == "__main__":
    result_df = calculate_service_time_from_columns()
    print("\nService Time calculation from columns complete!")
