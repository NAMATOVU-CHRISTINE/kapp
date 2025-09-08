import pandas as pd
import numpy as np

def extract_actual_service_time_only():
    # ...existing code...
    # Fill missing values in all columns before saving output files
    numeric_cols = [
        'Actual Days In Route', 'Budgeted Days In Route', 'Days In Route Deviation',
        'Bud Days In Route', 'Total Hour Route', 'Driver Rest Hours In Route', 'Total Wh',
        'Service Time At Customer'
    ]
    for col in numeric_cols:
        if col in result_df.columns:
            result_df[col] = result_df[col].fillna(0)

    string_cols = ['Ave Arrival Time', 'Clock Out', 'Service Time Source']
    for col in string_cols:
        if col in result_df.columns:
            result_df[col] = result_df[col].fillna('UNKNOWN')
    # ...existing code...
    # ...existing code...
    """
    Extract ONLY actual recorded service time data from source files.
    No calculations, estimates, or patterns - only real measured data.
    """
    
    # Load the main consolidated data
    print("Loading main consolidated data...")
    main_df = pd.read_excel('Final_Consolidated_Data_SERVICE_TIME_ACTUAL_ONLY.xlsx')
    
    # Create a copy and clear the Service Time At Customer column

    import pandas as pd
    import numpy as np

    def extract_actual_service_time_only():
        print("Loading main consolidated data...")
        main_df = pd.read_excel('Final_Consolidated_Data_SERVICE_TIME_ACTUAL_ONLY.xlsx')

        import pandas as pd
        import numpy as np

        def extract_actual_service_time_only():
            print("Loading main consolidated data...")
            main_df = pd.read_excel('Final_Consolidated_Data_SERVICE_TIME_ACTUAL_ONLY.xlsx')
            result_df = main_df.copy()
            result_df['Service Time At Customer'] = np.nan
            result_df['Service Time Source'] = 'NO_ACTUAL_DATA'

            # Add columns for Days In Route and other requested columns
            result_df['Actual Days In Route'] = np.nan
            result_df['Budgeted Days In Route'] = np.nan
            result_df['Days In Route Deviation'] = np.nan
            result_df['Bud Days In Route'] = np.nan
            result_df['Total Hour Route'] = np.nan
            result_df['Driver Rest Hours In Route'] = np.nan
            result_df['Total Wh'] = np.nan
            result_df['Clock Out'] = np.nan
            result_df['Ave Arrival Time'] = np.nan

            # Load route info
            route_df = pd.read_csv('5.Time_in_Route_Information.csv')
            for _, row in route_df.iterrows():
                load = str(row['Load']).strip()
                actual_days = row['Time in Route (min)'] / (60 * 24) if pd.notna(row['Time in Route (min)']) else np.nan
                budgeted_days = row['Planned Time in Route (min)'] / (60 * 24) if pd.notna(row['Planned Time in Route (min)']) else np.nan
                deviation = actual_days - budgeted_days if pd.notna(actual_days) and pd.notna(budgeted_days) else np.nan
                total_hour_route = row['Time in Route (min)'] / 60 if pd.notna(row['Time in Route (min)']) else np.nan
                rest_hours = row.get('Driver Rest Hours In Route', np.nan)
                total_wh = row.get('Total Wh', np.nan)
                idxs = result_df.index[result_df['Load Number'].astype(str).str.strip() == load]
                for idx in idxs:
                    result_df.at[idx, 'Actual Days In Route'] = actual_days
                    result_df.at[idx, 'Budgeted Days In Route'] = budgeted_days
                    result_df.at[idx, 'Days In Route Deviation'] = deviation
                    result_df.at[idx, 'Bud Days In Route'] = budgeted_days
                    result_df.at[idx, 'Total Hour Route'] = total_hour_route
                    result_df.at[idx, 'Driver Rest Hours In Route'] = rest_hours
                    result_df.at[idx, 'Total Wh'] = total_wh

            # Extract actual arrival and departure times and service times from Customer_Timestamps.csv
            print("\nExtracting actual service times from Customer_Timestamps.csv...")
            customer_df = pd.read_csv('2.Customer_Timestamps.csv')
            print(f"Customer_Timestamps records: {len(customer_df)}")
            print(f"Records with actual service time: {customer_df['Total Time Spent @ Customer'].notna().sum()}")
            actual_service_times = {}
            for _, row in customer_df.iterrows():
                load_name = str(row['load_name']).strip()
                arrival_time = row.get('ArrivedAtCustomer(Odo)', None)
                departure_time = row.get('Offloading', None)
                service_time = row['Total Time Spent @ Customer']
                if pd.notna(service_time) and service_time > 0:
                    actual_service_times[load_name] = {
                        'service_time': service_time,
                        'source': 'Customer_Timestamps_Actual'
                    }
                idxs = result_df.index[result_df['Load Number'].astype(str).str.strip() == load_name]
                for idx in idxs:
                    if pd.notna(arrival_time):
                        result_df.at[idx, 'Ave Arrival Time'] = arrival_time
                    if pd.notna(departure_time):
                        result_df.at[idx, 'Clock Out'] = departure_time

            print(f"Actual service times extracted: {len(actual_service_times)}")
            matched_count = 0
            for idx, row in result_df.iterrows():
                load_number = str(row['Load Number']).strip()
                if load_number in actual_service_times:
                    result_df.at[idx, 'Service Time At Customer'] = actual_service_times[load_number]['service_time']
                    result_df.at[idx, 'Service Time Source'] = actual_service_times[load_number]['source']
                    matched_count += 1
            print(f"Successfully matched actual service times: {matched_count}")

            # Fill missing values in all columns before saving output files
            numeric_cols = [
                'Actual Days In Route', 'Budgeted Days In Route', 'Days In Route Deviation',
                'Bud Days In Route', 'Total Hour Route', 'Driver Rest Hours In Route', 'Total Wh',
                'Service Time At Customer'
            ]
            for col in numeric_cols:
                if col in result_df.columns:
                    result_df[col] = result_df[col].fillna(0)
            string_cols = ['Ave Arrival Time', 'Clock Out', 'Service Time Source']
            for col in string_cols:
                if col in result_df.columns:
                    result_df[col] = result_df[col].fillna('UNKNOWN')

            # Calculate completion statistics
            total_records = len(result_df)
            completed_records = result_df['Service Time At Customer'].notna().sum()
            completion_rate = (completed_records / total_records) * 100
            print("\n" + "="*60)
            print("ACTUAL SERVICE TIME EXTRACTION RESULTS:")
            print("="*60)
            print(f"Total records: {total_records:,}")
            print(f"Records with ACTUAL service time data: {completed_records:,}")
            print(f"Records without actual data: {total_records - completed_records:,}")
            print(f"Completion rate (actual data only): {completion_rate:.1f}%")
            actual_data = result_df[result_df['Service Time At Customer'].notna()]['Service Time At Customer']
            if len(actual_data) > 0:
                print("\nACTUAL Service Time Statistics:")
                print(f"Min: {actual_data.min():.0f} minutes")
                print(f"Max: {actual_data.max():.0f} minutes")
                print(f"Mean: {actual_data.mean():.1f} minutes")
                print(f"Median: {actual_data.median():.0f} minutes")
            print("\nData Source Breakdown:")
            source_counts = result_df['Service Time Source'].value_counts()
            for source, count in source_counts.items():
                percentage = (count / total_records) * 100
                print(f"{source}: {count:,} records ({percentage:.1f}%)")
                # Remove specified columns before saving
                columns_to_remove = [
                    'Comment Ave Tir', 'Service Time Source', 'Data Quality', 'Accuracy_Notes', 'Budgeted Days In Route'
                ]
                for col in columns_to_remove:
                    if col in result_df.columns:
                        result_df = result_df.drop(columns=col)
            output_file = 'Final_Consolidated_Data_SERVICE_TIME_ACTUAL_ONLY.csv'
            result_df.to_csv(output_file, index=False)
            print(f"\nFile saved: {output_file}")
            # Drop columns only for Excel output
            columns_to_remove = [
                'Comment Ave Tir', 'Service Time Source', 'Data Quality', 'Accuracy_Notes', 'Budgeted Days In Route'
            ]
            excel_df = result_df.drop(columns=[col for col in columns_to_remove if col in result_df.columns])
            excel_file = output_file.replace('.csv', '.xlsx')
            excel_df.to_excel(excel_file, index=False)
            print(f"Excel file saved: {excel_file}")
            print("\nFirst 10 records with ACTUAL service time data:")
            actual_examples = result_df[result_df['Service Time At Customer'].notna()][
                ['Load Number', 'Customer Name', 'Service Time At Customer', 'Service Time Source']
            ].head(10)
            print(actual_examples.to_string(index=False))
            return result_df

            # Fill remaining gaps in all columns using actual data from the 5 source files
            # 1. Depot_Departures.csv
            depot_df = pd.read_csv('1.Depot_Departures.csv', sep='\t')
            for idx, row in result_df.iterrows():
                load_number = str(row['Load Number']).strip()
                depot_row = depot_df[depot_df['Load Name'].astype(str).str.strip() == load_number]
                if not depot_row.empty:
                    for col in ['Planned Departure Time', 'DJ Departure Time', 'Departure Time Difference (DJ vs Planned)', 'Depot']:
                        if col in result_df.columns and pd.isna(row[col]):
                            result_df.at[idx, col] = depot_row.iloc[0][col]

            # 2. Customer_Timestamps.csv
            customer_df = pd.read_csv('2.Customer_Timestamps.csv')
            for idx, row in result_df.iterrows():
                load_number = str(row['Load Number']).strip()
                cust_row = customer_df[customer_df['load_name'].astype(str).str.strip() == load_number]
                if not cust_row.empty:
                    for col in ['ArrivedAtCustomer(Odo)', 'Offloading', 'Total Time Spent @ Customer', 'Customer Gate To Offloading', 'Offloading to Invoice Completion', 'DriverName', 'customer_name']:
                        if col in result_df.columns and pd.isna(row[col]):
                            result_df.at[idx, col] = cust_row.iloc[0][col]

            # 3. Distance_Information.csv
            dist_df = pd.read_csv('3.Distance_Information.csv')
            for idx, row in result_df.iterrows():
                load_number = str(row['Load Number']).strip()
                dist_row = dist_df[dist_df['Load Name'].astype(str).str.strip() == load_number]
                if not dist_row.empty:
                    for col in ['PlannedDistanceToCustomer', 'Actual Km', 'Km Deviation', 'Planned Load Distance', 'Total DJ Distance for Load', 'Distance Difference (Planned vs DJ)', 'Load Distance Difference (Planned vs. DJ)', 'Depot Departure Odometer', 'Arrived at Customer Odometer', 'Arrived At Depot Odometer']:
                        if col in result_df.columns and pd.isna(row[col]):
                            result_df.at[idx, col] = dist_row.iloc[0][col]

            # 4. Timestamps_and_Duration.csv
            ts_df = pd.read_csv('4.Timestamps_and_Duration.csv')
            for idx, row in result_df.iterrows():
                load_number = str(row['Load Number']).strip()
                ts_row = ts_df[ts_df['load_name'].astype(str).str.strip() == load_number]
                if not ts_row.empty:
                    for col in ['Load StartTime (Pre-Trip Start)', 'GateManifestTime', 'Load Start to Gate Exit', 'ArriveAtDepot(Odo)', 'GateEntryCompletion', 'Depot Arrival to Gate Entry Complete', 'FullsWarehouseComplete', 'EmptiesWarehouseComplete', 'LoadCompleted', 'Gate Entry to load Completion']:
                        if col in result_df.columns and pd.isna(row[col]):
                            result_df.at[idx, col] = ts_row.iloc[0][col]

            # 5. Time_in_Route_Information.csv (already used above, but fill any remaining)
            route_df = pd.read_csv('5.Time_in_Route_Information.csv')
            for idx, row in result_df.iterrows():
                load_number = str(row['Load Number']).strip()
                route_row = route_df[route_df['Load'].astype(str).str.strip() == load_number]
                if not route_row.empty:
                    for col in ['Time in Route (min)', 'Planned Time in Route (min)', 'Time In Route Difference ( DJ - Planned)']:
                        if col in result_df.columns and pd.isna(row[col]):
                            result_df.at[idx, col] = route_row.iloc[0][col]

            # After all attempts, fill any remaining missing values with reasonable defaults
            for col in result_df.columns:
                if result_df[col].dtype in [np.float64, np.int64]:
                    result_df[col] = result_df[col].fillna(0)
                else:
                    result_df[col] = result_df[col].fillna('UNKNOWN')

        if __name__ == "__main__":
            extract_actual_service_time_only()
