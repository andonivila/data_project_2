FROM python:3.8.16-slim

WORKDIR /app

COPY taxi.py taxi.py
COPY requirements.txt requirements.txt
COPY data-project-2-376316-a19138ce1e45.json data-project-2-376316-a19138ce1e45.json

RUN pip install -r requirements.txt
RUN export GOOGLE_APPLICATION_CREDENTIALS=/app/data-project-2-376316-a19138ce1e45.json
 

CMD [ "python", "-u", "taxi.py" ]