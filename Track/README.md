
# Track Data Combiner App

Welcome! This is a personalized app  that will combine 5 key logistics data files into one consolidated output. The goal is to streamline your workflow, automate data merging, and provide a user-friendly experience for managing and analyzing route completion data.


## Main Data Files
- **1.Depot_Departures.csv**: Departure information from depots
- **2.Customer_Timestamps.csv**: Customer visit timestamps
- **3.Distance_Information.csv**: Distance data between locations
- **4.Timestamps_and_Duration.csv**: Timestamps and duration of activities
- **5.Time_in_Route_Information.csv**: Time spent on routes
- **Final_Consolidated_Data_Complete_Distance_NEW.xlsx** / **Final_Consolidated_Data_Complete_Distance.xlsx**: Example outputs


## App Scripts
These scripts are the building blocks for your app:
- **final.py / improved_final.py / final_with_customer_mapping.py**: Core scripts for combining the 5 main files into a single, unified dataset.
- **achieve_100_percent_completion.py / achieve_100_percent_service_time_actual.py / achieve_100_percent_with_actual_data.py**: Ensure data completeness and accuracy.
- **analyze_completion.py / departure_analysis.py**: Analyze completion and departure data.
- **calculate_departure_fields.py / calculate_service_time_from_columns.py**: Derive additional fields for analysis.
- **fill_*.py / fix_*.py / update_*.py**: Fill missing data, fix inconsistencies, and update records.
- **extract_actual_service_time_only.py / use_actual_data_only.py**: Work with actual data only.
- **verify_*.py**: Verify data integrity and matching.


## How to Use
1. Place your 5 main CSV files in the app directory.
2. Run the main app script (e.g., `final.py` or `improved_final.py`) to combine them into a single output file.
3. Use the other scripts to analyze, verify, and enhance your data as needed.


## Notes & Personalization
- This project is designed to be flexible and tailored to your workflow.
- Scripts are modular—run them independently or in sequence as you prefer.
- Make sure you have Python and required packages (like `pandas`, `openpyxl`) installed.


---
For more details on each script, check the script's docstrings or comments. If you need new features or a more user-friendly interface, feel free to expand and personalize the app further!
