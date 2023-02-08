FROM python:3.8.16-slim

WORKDIR /app

COPY user.py user.py
COPY requirements.txt requirements.txt
COPY arctic-cursor-376010-87f1bcaf048d.json arctic-cursor-376010-87f1bcaf048d.json 

RUN pip install -r requirements.txt

CMD [ "python", "-u", "user.py" ]