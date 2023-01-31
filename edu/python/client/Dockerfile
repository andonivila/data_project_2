FROM python:3.8.16-slim

WORKDIR /app

COPY main.py main.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD [ "python", "-u", "main.py" ]