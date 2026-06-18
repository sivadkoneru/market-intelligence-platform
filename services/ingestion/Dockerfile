FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY services/ingestion/requirements.txt ./services/ingestion/requirements.txt
RUN pip install --no-cache-dir -r services/ingestion/requirements.txt

COPY libs ./libs
COPY services ./services

USER appuser

EXPOSE 8001

CMD ["uvicorn", "services.ingestion.app:app", "--host", "0.0.0.0", "--port", "8001"]
