FROM docker.dbc.dk/dbc-python3

RUN useradd -m python
USER python
WORKDIR /home/python

ENV PATH=/home/python/.local/bin:$PATH

COPY src src
COPY setup.py setup.py
COPY MANIFEST.in MANIFEST.in
COPY docs docs
COPY docker/start.sh start.sh

RUN pip install --user pip && \
    pip install --user .

CMD ["./start.sh"]

LABEL SOLR_URL url for solr

EXPOSE 5000
