
from flask import Flask, render_template, render_template_string, send_file
import subprocess
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run', methods=['POST'])
def run_script():
    # Run the data processing script
    result = subprocess.run([
        os.path.join(os.getcwd(), '.venv/bin/python'),
        'final.py'
    ], capture_output=True, text=True)
    output = result.stdout + '\n' + result.stderr
    return render_template_string('''
        <h2>Script Output</h2>
        <pre>{{output}}</pre>
        <a href="/">Back</a>
    ''', output=output)

@app.route('/download/excel')
def download_excel():
    return send_file('Final_Consolidated_Data_Complete_Distance_NEW.xlsx', as_attachment=True)

@app.route('/download/csv')
def download_csv():
    return send_file('Final_Consolidated_Data_Complete_Distance.csv', as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
