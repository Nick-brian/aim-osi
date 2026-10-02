FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8000 DATABASE_PATH=/app/data/aimosi.sqlite3
WORKDIR /app
COPY app.py ./app.py
COPY web ./web
RUN useradd --system --uid 10001 appuser && mkdir -p /app/data && chown -R appuser:appuser /app
USER appuser
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:8000/health', timeout=2)"
CMD ["python", "app.py"]
