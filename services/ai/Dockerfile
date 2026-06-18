FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY services/ai/requirements.txt ./services/ai/requirements.txt
RUN pip install --no-cache-dir -r services/ai/requirements.txt

COPY libs ./libs
COPY services ./services

USER appuser

EXPOSE 8003

CMD ["uvicorn", "services.ai.app:app", "--host", "0.0.0.0", "--port", "8003"]
