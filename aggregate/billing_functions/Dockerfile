FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

# Port
ARG PORT=8080

# HOME
ENV APP_HOME /app
WORKDIR $APP_HOME

# Add files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY main.py .

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
EXPOSE $PORT
