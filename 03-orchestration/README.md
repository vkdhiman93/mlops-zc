
# MLOps Pipeline with Prefect

This project demonstrates how to implement a simple MLOps pipeline using Prefect, replacing an equivalent Airflow pipeline.

## 📦 Features
- Reads NYC Taxi data
- Preprocesses and engineers features
- Trains a linear regression model
- Logs metrics and model artifacts with MLflow
- Registers the best model based on RMSE

## 🛠️ Setup Instructions

### 1. Clone this Repo
Unzip the folder and `cd` into the project directory.

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Start MLflow Server (in a new terminal)
```bash
mlflow ui --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns
```

### 5. Run the Prefect Flow
```bash
python pipeline.py
```

## 🗃️ Project Structure
```
.
├── pipeline.py        # Main pipeline script
├── README.md          # This file
└── models/            # Folder for saved artifacts (created at runtime)
```

## ✅ Requirements
- Python 3.8+
- Prefect 2.x
- MLflow
- scikit-learn
- pandas
- pyarrow

Install all with:
```bash
pip install prefect mlflow scikit-learn pandas pyarrow
```

## 🧪 Sample Run
```bash
python pipeline.py
```

The model artifacts and logs will be saved in the `models/` and MLflow UI will show metrics.

Enjoy!
