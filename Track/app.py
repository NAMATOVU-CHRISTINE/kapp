from flask import Flask, render_template, request, send_file
import subprocess
import os
import sys

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.getcwd()

# Map form field names to the required filenames
FILE_MAPPING = {
    'file_depot': '1.Depot_Departures.csv',
    'file_customer': '2.Customer_Timestamps.csv',
    'file_distance': '3.Distance_Information.csv',
    'file_timestamps': '4.Timestamps_and_Duration.csv',
    'file_route': '5.Time_in_Route_Information.csv'
}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run', methods=['POST'])
def run_script():
    # 1. Save uploaded files
    uploaded_count = 0
    for field_name, target_name in FILE_MAPPING.items():
        if field_name in request.files:
            file = request.files[field_name]
            if file and file.filename != '':
                save_path = os.path.join(app.config['UPLOAD_FOLDER'], target_name)
                file.save(save_path)
                uploaded_count += 1
    
    # 2. Run the combiner script
    python_executable = sys.executable
    
    # Run the data processing script
    try:
        result = subprocess.run([
            python_executable,
            'data_combiner.py'
        ], capture_output=True, text=True, check=False)
        
        output = result.stdout + '\n' + result.stderr
        
        # Add a note about uploads
        upload_note = f"\n[System] Processed {uploaded_count} new uploaded files.\n"
        output = upload_note + output
        
    except Exception as e:
        output = f"Critical Error running script: {str(e)}"

    return render_template('result.html', output=output)

@app.route('/download/excel')
def download_excel():
    path = 'Final_Consolidated_Data_Complete.xlsx'
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return "File not found. Please run the script first.", 404

@app.route('/download/csv')
def download_csv():
    path = 'Final_Consolidated_Data_Complete.csv'
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return "File not found. Please run the script first.", 404

if __name__ == '__main__':
    app.run(debug=True, port=5000)
