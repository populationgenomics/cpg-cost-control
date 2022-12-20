FROM python:3.10

WORKDIR root

# Add files
COPY *.py .
COPY main.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
