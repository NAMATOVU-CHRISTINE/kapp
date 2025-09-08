import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def fill_service_time_with_actual_data():
    """
    Fill missing Service Time At Customer using actual data from source files
    """
    print("Loading consolidated data...")
    # Read the main consolidated data
    df = pd.read_csv('Final_Consolidated_Data_COMPLETE_DEPARTURES.csv')
    
    print("Loading source files...")
    # Read source files that contain service time data
    customer_timestamps = pd.read_csv('2.Customer_Timestamps.csv')
    
    print("Processing Customer Timestamps data...")
    # Clean and prepare the customer timestamps data
    customer_timestamps['load_name'] = customer_timestamps['load_name'].str.strip()
    customer_timestamps['Total Time Spent @ Customer'] = pd.to_numeric(
        customer_timestamps['Total Time Spent @ Customer'], errors='coerce'
    )
    
    # Create mapping from load number to service time
    service_time_mapping = {}
    for _, row in customer_timestamps.iterrows():
        load_num = row['load_name']
        service_time = row['Total Time Spent @ Customer']
        if pd.notna(service_time) and pd.notna(load_num):
            service_time_mapping[load_num] = service_time
    
    print(f"Found {len(service_time_mapping)} service time records in source files")
    
    # Count current status
    total_records = len(df)
    missing_before = df['Service Time At Customer'].isna().sum()
    has_data_before = total_records - missing_before
    
    print(f"\nBefore filling:")
    print(f"Total records: {total_records}")
    print(f"Records with service time: {has_data_before}")
    print(f"Records missing service time: {missing_before}")
    print(f"Completion: {(has_data_before/total_records)*100:.1f}%")
    
    # Fill missing service times with actual data
    filled_count = 0
    
    for idx, row in df.iterrows():
        if pd.isna(row['Service Time At Customer']):
            load_num = row['Load Number']
            if load_num in service_time_mapping:
                df.at[idx, 'Service Time At Customer'] = service_time_mapping[load_num]
                filled_count += 1
    
    print(f"\nFilled {filled_count} records with actual source data")
    
    # Count after filling with actual data
    missing_after_actual = df['Service Time At Customer'].isna().sum()
    has_data_after_actual = total_records - missing_after_actual
    
    print(f"\nAfter filling with actual data:")
    print(f"Records with service time: {has_data_after_actual}")
    print(f"Records still missing service time: {missing_after_actual}")
    print(f"Completion: {(has_data_after_actual/total_records)*100:.1f}%")
    
    # For remaining missing records, use intelligent pattern-based approach
    if missing_after_actual > 0:
        print(f"\nFilling remaining {missing_after_actual} records using intelligent patterns...")
        
        # Build patterns based on actual service times by customer and driver
        customer_patterns = {}
        driver_patterns = {}
        
        # Analyze existing service times
        valid_service_times = df[df['Service Time At Customer'].notna()]
        
        # Customer-based patterns
        for customer in valid_service_times['Customer Name'].unique():
            customer_data = valid_service_times[valid_service_times['Customer Name'] == customer]
            if len(customer_data) >= 3:  # Need at least 3 records for reliable pattern
                service_times = customer_data['Service Time At Customer'].values
                # Use median for robust average
                avg_service_time = np.median(service_times)
                customer_patterns[customer] = avg_service_time
        
        # Driver-based patterns
        for driver in valid_service_times['Driver Name'].unique():
            driver_data = valid_service_times[valid_service_times['Driver Name'] == driver]
            if len(driver_data) >= 3:  # Need at least 3 records for reliable pattern
                service_times = driver_data['Service Time At Customer'].values
                avg_service_time = np.median(service_times)
                driver_patterns[driver] = avg_service_time
        
        # Calculate overall median as fallback
        overall_median = np.median(valid_service_times['Service Time At Customer'].values)
        
        print(f"Built patterns for {len(customer_patterns)} customers and {len(driver_patterns)} drivers")
        print(f"Overall median service time: {overall_median:.1f} minutes")
        
        # Fill remaining missing values using patterns
        pattern_filled = 0
        
        for idx, row in df.iterrows():
            if pd.isna(row['Service Time At Customer']):
                customer = row['Customer Name']
                driver = row['Driver Name']
                
                # Priority: Customer pattern > Driver pattern > Overall median
                if customer in customer_patterns:
                    df.at[idx, 'Service Time At Customer'] = customer_patterns[customer]
                    pattern_filled += 1
                elif driver in driver_patterns:
                    df.at[idx, 'Service Time At Customer'] = driver_patterns[driver]
                    pattern_filled += 1
                else:
                    df.at[idx, 'Service Time At Customer'] = overall_median
                    pattern_filled += 1
        
        print(f"Filled {pattern_filled} records using intelligent patterns")
    
    # Final count
    missing_final = df['Service Time At Customer'].isna().sum()
    has_data_final = total_records - missing_final
    
    print(f"\nFinal Results:")
    print(f"Total records: {total_records}")
    print(f"Records with service time: {has_data_final}")
    print(f"Records missing service time: {missing_final}")
    print(f"Completion: {(has_data_final/total_records)*100:.1f}%")
    
    # Show service time statistics
    service_times = df['Service Time At Customer'].dropna()
    print(f"\nService Time Statistics:")
    print(f"Min: {service_times.min():.1f} minutes")
    print(f"Max: {service_times.max():.1f} minutes")
    print(f"Median: {service_times.median():.1f} minutes")
    print(f"Mean: {service_times.mean():.1f} minutes")
    
    # Save the updated data
    print("\nSaving updated files...")
    df.to_csv('Final_Consolidated_Data_SERVICE_TIME_COMPLETE.csv', index=False)
    df.to_excel('Final_Consolidated_Data_SERVICE_TIME_COMPLETE.xlsx', index=False)
    
    print("Files saved:")
    print("- Final_Consolidated_Data_SERVICE_TIME_COMPLETE.csv")
    print("- Final_Consolidated_Data_SERVICE_TIME_COMPLETE.xlsx")
    
    return df

if __name__ == "__main__":
    result_df = fill_service_time_with_actual_data()
    print("\nService Time At Customer completion process finished!")
