import pandas as pd
import numpy as np

def fill_service_time_actual_data_only():
    """
    Fill Service Time At Customer using ONLY actual data from source files
    No predictions, no patterns - only real data that exists in the files
    """
    print("=== FILLING SERVICE TIME AT CUSTOMER - ACTUAL DATA ONLY ===")
    print("Strategy: Use only actual data from source files, no predictions")
    print()
    
    # Read the current consolidated data
    df = pd.read_csv('Final_Consolidated_Data_SERVICE_TIME_COMPLETE.csv')
    
    print(f"📊 Total records: {len(df)}")
    
    # Read source files that contain actual service time data
    print("\n📂 Loading source files with actual service time data...")
    
    # Source 1: Customer_Timestamps.csv - has "Total Time Spent @ Customer"
    customer_timestamps = pd.read_csv('2.Customer_Timestamps.csv')
    print(f"   ✅ Customer_Timestamps.csv: {len(customer_timestamps)} records")
    
    # Clean and prepare the customer timestamps data
    customer_timestamps['load_name'] = customer_timestamps['load_name'].str.strip()
    customer_timestamps['Total Time Spent @ Customer'] = pd.to_numeric(
        customer_timestamps['Total Time Spent @ Customer'], errors='coerce'
    )
    
    # Create mapping from load number to actual service time
    actual_service_times = {}
    
    for _, row in customer_timestamps.iterrows():
        load_num = str(row['load_name']).strip()
        service_time = row['Total Time Spent @ Customer']
        
        if pd.notna(service_time) and pd.notna(load_num) and load_num != '':
            actual_service_times[load_num] = service_time
    
    print(f"   ✅ Found {len(actual_service_times)} actual service time records")
    
    # Reset Service Time At Customer to use only actual data
    print("\n🔄 Resetting to use only actual source data...")
    
    # Create a new column with only actual data
    df['Service Time At Customer - Actual Only'] = np.nan
    
    # Fill with actual data only
    actual_data_filled = 0
    
    for idx, row in df.iterrows():
        load_num = str(row['Load Number']).strip()
        
        if load_num in actual_service_times:
            df.at[idx, 'Service Time At Customer - Actual Only'] = actual_service_times[load_num]
            actual_data_filled += 1
    
    print(f"   ✅ Filled {actual_data_filled} records with actual source data")
    
    # Replace the original column with actual-only data
    df['Service Time At Customer'] = df['Service Time At Customer - Actual Only']
    df = df.drop('Service Time At Customer - Actual Only', axis=1)
    
    # Calculate completion statistics
    total_records = len(df)
    actual_filled = df['Service Time At Customer'].notna().sum()
    missing_records = total_records - actual_filled
    completion_rate = (actual_filled / total_records) * 100
    
    print(f"\n📊 ACTUAL DATA ONLY RESULTS:")
    print(f"   Total records: {total_records}")
    print(f"   Records with actual service time: {actual_filled}")
    print(f"   Records missing (no actual data available): {missing_records}")
    print(f"   Completion rate: {completion_rate:.1f}%")
    
    # Show statistics of actual service times
    actual_times = df['Service Time At Customer'].dropna()
    if len(actual_times) > 0:
        print(f"\n📈 ACTUAL SERVICE TIME STATISTICS:")
        print(f"   Minimum: {actual_times.min():.1f} minutes ({actual_times.min()/60:.1f} hours)")
        print(f"   Maximum: {actual_times.max():.1f} minutes ({actual_times.max()/60:.1f} hours)")
        print(f"   Median: {actual_times.median():.1f} minutes ({actual_times.median()/60:.1f} hours)")
        print(f"   Average: {actual_times.mean():.1f} minutes ({actual_times.mean()/60:.1f} hours)")
    
    # Save the actual-data-only version
    output_csv = "Final_Consolidated_Data_SERVICE_TIME_ACTUAL_ONLY.csv"
    output_excel = "Final_Consolidated_Data_SERVICE_TIME_ACTUAL_ONLY.xlsx"
    
    df.to_csv(output_csv, index=False)
    df.to_excel(output_excel, index=False)
    
    print(f"\n💾 FILES SAVED:")
    print(f"   📄 CSV: {output_csv}")
    print(f"   📊 Excel: {output_excel}")
    
    print(f"\n✅ COMPLETED: Service Time At Customer now contains ONLY actual data from source files")
    print("   - No predictions or patterns used")
    print("   - Only real operational data from Customer_Timestamps.csv")
    print(f"   - {completion_rate:.1f}% completion with verified actual data")
    
    return df

if __name__ == "__main__":
    result_df = fill_service_time_actual_data_only()
    print("\nActual data only process complete!")
