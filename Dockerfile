FROM python:3.12-slim-trixie
LABEL authors=""
RUN apt update && apt install -y vim curl build-essential
RUN curl -sSL https://install.python-poetry.org | python3 -
COPY . /root/reflow
RUN cd /root/reflow && /root/.local/bin/poetry install
WORKDIR /root/reflow
ENTRYPOINT ["/root/.local/bin/poetry","run","python", "-m", "reflow.flow_engine"]

