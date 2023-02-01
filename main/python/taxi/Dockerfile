FROM python:3.8.16-slim

WORKDIR /app

COPY taxi.py taxi.py
COPY requirements.txt requirements.txt
COPY data-project-2-376316-7ec597825415.json data-project-2-376316-7ec597825415.json

RUN pip install -r requirements.txt

CMD [ "python", "-u", "taxi.py" ]