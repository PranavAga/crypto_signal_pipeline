"""
run.py - Pipeline for cryptocurrency signal generation.

Usage:
    $ python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
"""
import pandas as pd
import numpy as np
import yaml
import argparse
import json
import logging
import time
import os

def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def run_pipeline():
    start_time = time.time()
    
    # CLI Argument Parsing
    parser = argparse.ArgumentParser(description="Pipeline for cryptocurrency signal generation")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--config", required=True, help="Path to config YAML")
    parser.add_argument("--output", required=True, help="Path for metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path for log file")
    args = parser.parse_args()

    setup_logging(args.log_file)
    logging.info("Job started")

    metrics = {
            "version": config.get('version', 'unknown') if 'config' in locals() else 'unknown',
            "rows_processed": 0,
            "metric": "signal_rate",
            "value": 0.0,
            "latency_ms": 0,
            "seed": 0,
            "status": "success"
        }

    try:
        # Configuration Loading
        if not os.path.exists(args.config):
            raise FileNotFoundError(f"Config file not found: {args.config}")
        
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        
        seed = config.get('seed')
        window = config.get('window')
        version = config.get('version')
        
        metrics['seed'] = seed
        metrics['version'] = version
        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # Reproducibility
        np.random.seed(seed)

        # Data Ingestion
        if not os.path.exists(args.input):
            raise FileNotFoundError(f"Input file not found: {args.input}")
            
        df = pd.read_csv(args.input)
        if df.empty:
            raise ValueError("Input CSV file is empty")
        if 'close' not in df.columns:
            raise KeyError("Missing required 'close' column")
        
        rows_processed = len(df)
        metrics['rows_processed'] = rows_processed
        logging.info(f"Data loaded: {rows_processed} rows")

        # Rolling Mean Computation
        logging.info(f"Calculating rolling mean with window={window}")
        df['rolling_mean'] = df['close'].rolling(window=window).mean()

        # Signal Generation
        # Signal 1 if close > rolling_mean, else 0. Handles NaNs by defaulting to 0.
        logging.info("Generating signals")
        df['signal'] = (df['close'] > df['rolling_mean']).astype(int)
        
        # Metrics Calculation
        signal_rate = float(df['signal'].mean())
        metrics['metric'] = "signal_rate"
        metrics['value'] = round(signal_rate, 4)
        
        end_time = time.time()
        latency_ms = int((end_time - start_time) * 1000)
        metrics['latency_ms'] = latency_ms

        logging.info(f"Metrics: signal_rate={metrics['value']} rows_processed={rows_processed}")
        logging.info(f"Job completed successfully in {latency_ms}ms")

    except Exception as e:
        logging.error(f"Error during execution: {str(e)}")
        metrics = {
            "version": config.get('version', 'unknown') if 'config' in locals() else 'unknown',
            "status": "error",
            "error_message": str(e)
        }
    
    # Write JSON Output
    with open(args.output, 'w') as f:
        json.dump(metrics, f, indent=4)
    
    print(json.dumps(metrics, indent=4))

if __name__ == "__main__":
    run_pipeline()