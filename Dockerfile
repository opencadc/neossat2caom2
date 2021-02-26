FROM opencadc/matplotlib:3.8-slim

RUN apt-get update -y && apt-get dist-upgrade -y && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/ /tmp/* /var/tmp/*

RUN pip install cadcdata \
  cadctap \
  caom2 \
  caom2repo \
  caom2utils \
  ftputil \
  importlib-metadata \
  python-dateutil \
  PyYAML \
  spherical-geometry \
  vos

WORKDIR /usr/src/app

ARG OPENCADC_BRANCH=master
ARG OPENCADC_REPO=opencadc
ARG PIPE_BRANCH=master
ARG PIPE_REPO=opencadc

RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe
  
RUN pip install git+https://github.com/${PIPE_REPO}/neossat2caom2@${PIPE_BRANCH}#egg=neossat2caom2

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

