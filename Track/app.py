from flask import Flask, render_template, render_template_string, send_file
import subprocess
import os
import sys

app = Flask(__name__)

@app.route('/upload')
def upload_page():
    return render_template('base_upload.html')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run', methods=['POST'])
def run_script():
    # Use the same python executable that is running this script
    python_executable = sys.executable
    
    # Run the data processing script
    result = subprocess.run([
        python_executable,
        'data_combiner.py'
    ], capture_output=True, text=True)
    
    output = result.stdout + '\n' + result.stderr
    return render_template_string('''
        <h2>Script Output</h2>
        <pre>{{output}}</pre>
        <div style="margin-top: 20px;">
            <a href="/" style="padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px;">Back to Home</a>
        </div>
    ''', output=output)

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
