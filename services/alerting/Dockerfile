FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY services/alerting/requirements.txt ./services/alerting/requirements.txt
RUN pip install --no-cache-dir -r services/alerting/requirements.txt

COPY libs ./libs
COPY services ./services

USER appuser

EXPOSE 8004

CMD ["uvicorn", "services.alerting.app:app", "--host", "0.0.0.0", "--port", "8004"]
