# Backend + UI image. The repository is mounted at /app (docker-compose.yml), so code, data, rules,
# gold/ and fixtures/ are always the checked-out ones; only Python dependencies are baked in.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app/backend
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "-m", "uvicorn", "pipeline.web.app:app", "--host", "0.0.0.0", "--port", "8000"]
