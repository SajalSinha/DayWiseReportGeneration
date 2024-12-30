from flask import Flask, request, send_file, jsonify
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

# Directory to store temporary files
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/process', methods=['POST'])
def process():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    # Save uploaded file with timestamp to avoid overwrites
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    input_filename = f"{UPLOAD_FOLDER}/input_{timestamp}.csv"
    file.save(input_filename)

    # Read the CSV
    try:
        df = pd.read_csv(input_filename)
    except Exception as e:
        return jsonify({"error": f"Failed to read file: {str(e)}"}), 400

    # Process the CSV
    try:
        df['Activity Updated Date'] = pd.to_datetime(df['Activity Updated Date'], errors='coerce')
        df['Date'] = df['Activity Updated Date'].dt.date

        # Generate daily performance
        day_wise = (
            df.groupby(['Date', 'Activity Updated By'])
            .size()
            .reset_index(name='Survey Count')
        )

        # Pivot to time series
        time_series = day_wise.pivot(index='Date', columns='Activity Updated By', values='Survey Count').fillna(0)

        # Save output file with timestamp
        output_filename = f"{UPLOAD_FOLDER}/output_{timestamp}.csv"
        time_series.to_csv(output_filename)

        return send_file(output_filename, as_attachment=True)
    except Exception as e:
        return jsonify({"error": f"Failed to process file: {str(e)}"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
