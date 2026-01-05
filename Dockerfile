FROM python:3.11-slim

WORKDIR /app

# Install required Python packages
RUN pip install --no-cache-dir requests pandas

# Copy the Python script
COPY fetch_data.py .

# Run the script
CMD ["python", "fetch_data.py"]
