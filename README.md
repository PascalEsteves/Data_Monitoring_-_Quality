
# 📊 Data Quality Monitoring Pipeline

This project implements a system for **validating and monitoring data quality** in a continuous pipeline. Its goal is to ensure that data adheres to expected standards and to trigger **automatic alerts** when anomalies or deviations are detected.

## 🚀 Features

- ✅ Automatic validation of datasets based on configurable rules
- 📈 Continuous monitoring of data quality
- ⚠️ Real-time alerting when data falls outside expected thresholds ( not implemented yet)
- 📂 Easy integration with existing data pipelines

## 🛠️ Technologies Used

- Python 3.x
- [PyYAML](https://pyyaml.org/) for rule configuration
- Pandas for data manipulation
- Logging for tracking events and alerts

## 📦 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/PascalEsteves/Data_Monitoring_-_Quality.git
   cd Data_Monitoring_-_Quality
   
2. Create and activate virtual environment
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate

3. Install Dependencies
    ```bash
    pip install -r requirements.txt


## How to use

1. Define validation rules in the config.yaml ( example in configs/validation.yaml)
2. Run Script
    ```bash 
    python run_validation.py -p "path_to_config.yaml'
3. Check Results in results/results.json

