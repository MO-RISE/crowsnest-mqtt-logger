FROM python:3.10-slim

COPY requirements.txt requirements.txt
COPY main.py main.py

RUN pip3 install -r requirements.txt
RUN mkdir -p /DB

CMD ["python3", "main.py"]