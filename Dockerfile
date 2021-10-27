FROM python:3
MAINTAINER Harold Woo <hwoo@mozilla.com>

ENV PYTHONUNBUFFERED=1

ARG APP_NAME=probe-scraper
ENV APP_NAME=${APP_NAME}

# Guidelines here: https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir /app ${GROUP_ID}

# List packages here
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        file        \
        gcc         \
        libwww-perl && \
    apt-get autoremove -y && \
    apt-get clean

# Upgrade pip
RUN pip install --upgrade pip

WORKDIR ${HOME}

COPY requirements.txt ${HOME}/
RUN pip install -r requirements.txt

COPY test_requirements.txt ${HOME}/
RUN pip install -r test_requirements.txt

COPY . ${HOME}
RUN pip install .

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}
