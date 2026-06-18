FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY services/stream/requirements.txt ./services/stream/requirements.txt
RUN pip install --no-cache-dir -r services/stream/requirements.txt

COPY libs ./libs
COPY services ./services

USER appuser

EXPOSE 8002

CMD ["uvicorn", "services.stream.app:app", "--host", "0.0.0.0", "--port", "8002"]
