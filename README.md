# Setup Instructions
```bash
# Install dependencies
pip install -r requirements.txt
```

# Local Execution Instructions:
```bash
# Run locally
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

# Docker Instructions:
```bash
# Build Docker image
docker build -t mlops-task .

# Run Docker container
docker run --rm mlops-task
```

# Expected Output:
The output will be a JSON file containing the calculated metrics, which will look like this:
```json
{
    "version": "1.0",
    "rows_processed": 100,
    "metric": "signal_rate",
    "value": 0.45,
    "latency_ms": 150,
    "seed": 42,
    "status": "success"
}
```

# Dependencies:
- Python 3.12+
- pandas
- numpy
- PyYAML

