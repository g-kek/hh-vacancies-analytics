FROM python:3.12-slim@sha256:191083cdf6c7bda8c76158983163ead5d5cf7b77cf3033e805bb0f5296f51b4b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.lock.txt ./
RUN python -m pip install --no-cache-dir -r requirements.lock.txt

COPY config/ config/
COPY src/ src/

USER 10001:10001

ENTRYPOINT ["python", "-m", "hh_analytics.kubernetes"]
