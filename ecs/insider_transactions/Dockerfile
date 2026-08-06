FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collect_insider_transactions.py .

CMD ["python", "collect_insider_transactions.py"]
