FROM prefecthq/prefect:2-latest

ARG WEATHER_API_KEY

ENV API_KEY=$WEATHER_API_KEY

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir \
    && pip install --upgrade numpy pandas

COPY flows /opt/prefect/flows
COPY data /opt/prefect/data
COPY terraform /opt/prefect/terraform
COPY blocks /opt/prefect/blocks
