# Build from streaming-app/ context: docker build -f load-generator/Dockerfile .
FROM python:3.12-slim

WORKDIR /app

COPY load-generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load-generator/ /app/load-generator/
COPY schemas/ /app/schemas/

WORKDIR /app/load-generator

CMD ["python", "generator.py"]
