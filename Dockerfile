FROM python:2-slim

RUN apt-get update
RUN apt-get install -y git libpq-dev postgresql-client gcc

WORKDIR /usr/src/app
COPY src/ /usr/src/app/
RUN find /usr/src/app -name "*.py"|xargs chmod +x

ENV PATH="/usr/src/app/AnalyzeVacuumUtility:/usr/src/app/ColumnEncodingUtility:/usr/src/app/UnloadCopyUtility:${PATH}"

RUN pip install -r /usr/src/app/requirements.txt


