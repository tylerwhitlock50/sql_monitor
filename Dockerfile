FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
ENV PYTHONPATH=/app/src
ENV APP_MODE=collector
ENV WEB_HOST=0.0.0.0
ENV WEB_PORT=8000

EXPOSE 8000

CMD ["sh", "-c", "if [ \"$APP_MODE\" = \"web\" ]; then python -m sql_monitor.web; else python -m sql_monitor; fi"]
