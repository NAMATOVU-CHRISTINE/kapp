# LogiFusion


Welcome! This is a simplified and robust app that combines your 5 key logistics data files into one consolidated output.

## How to Use

### Option 1: Command Line
1. Ensure your 5 CSV files are in the folder:
   - `1.Depot_Departures.csv`
   - `2.Customer_Timestamps.csv`
   - `3.Distance_Information.csv`
   - `4.Timestamps_and_Duration.csv`
   - `5.Time_in_Route_Information.csv`
2. Run the combiner script:
   ```bash
   .venv/bin/python data_combiner.py
   ```
3. The consolidated file will be saved as `Final_Consolidated_Data_Complete.xlsx`.

### Option 2: Web Interface
1. Run the web app:
   ```bash
   .venv/bin/python app.py
   ```
2. Open your browser to `http://127.0.0.1:5000`.
3. Click "Run" to process the data.
4. Download the Excel or CSV results.

## System Details
- **data_combiner.py**: The main logic script. Reads all files, smart-match load numbers, fills missing data using historical patterns (medians, customer lookups), and calculates route statistics.
- **app.py**: Simple web interface for the tool.
- **requirements.txt**: List of Python dependencies.

## Data Logic
The system merges data primarily by **Load Number**. If data is missing (e.g. absent distance info), it attempts to fill gaps by:
1. Looking up other records from the same **Data Source**.
2. Calculating deviations based on **Customer** or **Driver** history.
3. Using median values for the specific Customer.

Enjoy your streamlined workflow!
