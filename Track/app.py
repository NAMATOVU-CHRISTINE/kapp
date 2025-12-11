from flask import Flask, render_template, request, send_file
import os
import sys
# Import the processing function directly
from data_combiner import process_data_func

app = Flask(__name__)

# Use /tmp for Vercel (and local) write permissions
# On local, you might want os.getcwd(), but /tmp is safer for stateless logic
# unless the user wants persistence. Let's use /tmp for compatibility.
app.config['UPLOAD_FOLDER'] = "/tmp"

# Ensure the upload folder exists
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

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
    try:
        # Ensure directory exists again just in case
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])

        for field_name, target_name in FILE_MAPPING.items():
            if field_name in request.files:
                file = request.files[field_name]
                if file and file.filename != '':
                    save_path = os.path.join(app.config['UPLOAD_FOLDER'], target_name)
                    file.save(save_path)
                    uploaded_count += 1
        
        # 2. Run the combiner logic directly
        try:
            # Capture output by intercepting stdout? Or just trust the return message.
            # The refactored function returns a status string.
            # If we really want stdout, we'd have to redirect it, but return message is safer.
            result_msg = process_data_func(app.config['UPLOAD_FOLDER'])
            output = result_msg
            
            # Add a note about uploads
            upload_note = f"\n[System] Processed {uploaded_count} news uploaded files.\n"
            output = upload_note + output
            
        except Exception as e:
            output = f"Critical Error running script: {str(e)}"
            import traceback
            traceback.print_exc()

        return render_template('result.html', output=output)

    except Exception as e:
        return f"Error handling request: {e}", 500

@app.route('/download/excel')
def download_excel():
    path = os.path.join(app.config['UPLOAD_FOLDER'], 'Final_Consolidated_Data_Complete.xlsx')
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return "File not found. Please run the script first.", 404

@app.route('/download/csv')
def download_csv():
    path = os.path.join(app.config['UPLOAD_FOLDER'], 'Final_Consolidated_Data_Complete.csv')
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return "File not found. Please run the script first.", 404

if __name__ == '__main__':
    app.run(debug=True, port=5000)
