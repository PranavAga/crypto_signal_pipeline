FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application files and data
COPY run.py .
COPY config.yaml .
COPY data.csv .

# Run the job automatically
CMD ["python", "run.py", \
     "--input", "data.csv", \
     "--config", "config.yaml", \
     "--output", "metrics.json", \
     "--log-file", "run.log"]