FROM opencadc/matplotlib:3.8-slim

RUN pip install cadcdata \
  cadctap \
  caom2 \
  caom2repo \
  caom2utils \
  ftputil \
  importlib-metadata \
  pytz \
  PyYAML \
  spherical-geometry \
  vos

WORKDIR /usr/src/app

RUN apt-get update -y && apt-get dist-upgrade -y

RUN apt-get install -y git

ARG OMC_REPO=opencadc-metadata-curation

RUN git clone https://github.com/${OMC_REPO}/caom2pipe.git --single-branch && \
  pip install ./caom2pipe
  
RUN git clone https://github.com/${OMC_REPO}/neossat2caom2.git --single-branch && \
  cp ./neossat2caom2/scripts/config.yml / && \
  cp ./neossat2caom2/scripts/state.yml / && \
  cp ./neossat2caom2/scripts/docker-entrypoint.sh / && \
  pip install ./neossat2caom2

ENTRYPOINT ["/docker-entrypoint.sh"]

