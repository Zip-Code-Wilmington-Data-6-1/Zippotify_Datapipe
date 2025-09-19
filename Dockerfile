FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.api.txt .
RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir -r requirements.api.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "fast_api:app", "--host", "0.0.0.0", "--port", "8000"]