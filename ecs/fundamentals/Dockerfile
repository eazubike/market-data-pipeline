FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY collect_fundamentals.py .

# Run the collection
CMD ["python", "collect_fundamentals.py"]
