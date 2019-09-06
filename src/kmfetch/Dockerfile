FROM python:alpine

ADD config.py .
ADD db.py .
ADD master_multiproc.py .
ADD requirements.txt .

WORKDIR .

RUN pip3 install -r requirements.txt

CMD ["python3", "kmfetch.py"]
