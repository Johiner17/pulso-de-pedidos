FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY s3_smoke.py .

ENTRYPOINT ["python", "/app/s3_smoke.py"]
