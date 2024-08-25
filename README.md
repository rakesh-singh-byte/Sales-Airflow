
---

# Airflow Project - Sales Data Pipeline

## Table of Contents
- [Airflow Project - Sales Data Pipeline](#airflow-project---sales-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Project Overview](#project-overview)
  - [Directory Structure](#directory-structure)
    - [Explanation of Directories and Files](#explanation-of-directories-and-files)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [Usage](#usage)
  - [DAGs Overview](#dags-overview)
  - [Testing](#testing)
  - [Troubleshooting](#troubleshooting)
  - [Contributing](#contributing)
  - [License](#license)
  - [Contact](#contact)

## Project Overview
This Airflow project is designed to automate the ETL process for sales data. It consists of a series of DAGs that load, process, and backup sales data, particularly focusing on liquor sales. The project leverages Airflow’s capabilities to orchestrate complex workflows and manage dependencies efficiently.

## Directory Structure
```
.
├── dags
│   └── sales
│       ├── load_liquor_sales_raw_data.py
│       ├── load_sales_raw_data.py
│       ├── process_liquor_sales_data.py
│       └── sales_config_backup.py
├── data
│   ├── connections.json
│   └── variables.json
├── plugins
├── requirements.txt
├── structure.txt
└── utils
    └── utils.py
```

### Explanation of Directories and Files
- **dags/**: Contains all DAG definitions, organized under the `sales/` subdirectory.
  - `load_liquor_sales_raw_data.py`: DAG for loading raw liquor sales data.
  - `load_sales_raw_data.py`: DAG for loading other sales raw data.
  - `process_liquor_sales_data.py`: DAG for processing liquor sales data.
  - `sales_config_backup.py`: DAG for backing up sales configurations.
  
- **data/**: Holds JSON files that store connections and variables used within Airflow.
  - `connections.json`: Contains connection settings for various data sources.
  - `variables.json`: Stores variables used in the DAGs.
  
- **plugins/**: Directory for custom plugins, operators, hooks, sensors, etc. (currently empty).
  
- **requirements.txt**: Lists Python dependencies required by the project.
  
- **structure.txt**: Provides a description of the project structure and files.
  
- **utils/**: Contains utility scripts that assist with the operations of the DAGs.
  - `utils.py`: Utility functions shared across multiple DAGs.

## Prerequisites
- Python 3.8+
- Apache Airflow 2.0+
- pip

## Installation
Follow these steps to set up the project on your local machine:

1. **Clone the repository:**
    ```bash
    git clone https://github.com/yourusername/your-repo-name.git
    cd your-repo-name
    ```

2. **Set up a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4. **Initialize the Airflow database:**
    ```bash
    export AIRFLOW_HOME=~/airflow
    airflow db init
    ```

5. **Start the Airflow web server and scheduler:**
    ```bash
    airflow webserver --port 8080
    airflow scheduler
    ```

## Configuration
- **Connections and Variables:** Ensure that `connections.json` and `variables.json` in the `data/` directory are correctly configured to reflect your environment. These files are automatically imported into Airflow when the DAGs run.

## Usage
- **Running the DAGs:**
  - Access the Airflow web interface at `http://localhost:8080`.
  - Trigger the DAGs manually or set up a schedule in the DAG definitions.

## DAGs Overview
- **`load_liquor_sales_raw_data.py`:** Loads raw liquor sales data into the data warehouse.
- **`load_sales_raw_data.py`:** Loads other sales raw data into the data warehouse.
- **`process_liquor_sales_data.py`:** Processes and cleans liquor sales data for analytics.
- **`sales_config_backup.py`:** Backs up sales configuration files and settings.

## Testing
- **Unit Tests:** Implement unit tests in the `tests/` directory to validate your DAGs and utilities.
- **Manual Testing:** Use the Airflow web UI to monitor and manually test the DAGs.

## Troubleshooting
- Check the logs in the `logs/` directory or via the Airflow web interface for any errors.
- Ensure all connections and variables are correctly set in Airflow.

## Contributing
1. Fork the repository.
2. Create a new branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Create a new Pull Request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or support, reach out to [rakesh.s.shankala@gmail.com](mailto:rakesh.s.shankala@gmail.com).

---