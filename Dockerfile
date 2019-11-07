FROM python:3.7

COPY . /

RUN pip install -r $DOCKYARD_SRVPROJ/requirements.txt

CMD ["python", "./scrape.py"]
