FROM opencadc/matplotlib:3.6-alpine

RUN apk --no-cache add bash \
    coreutils \
    git \
    g++ \
    libmagic

RUN pip install cadcdata && \
  pip install cadctap && \
  pip install caom2 && \
  pip install caom2repo && \
  pip install caom2utils && \
  pip install ftputil && \
  pip install pytz && \
  pip install PyYAML && \
  pip install spherical-geometry && \
  pip install vos

WORKDIR /usr/src/app

RUN git clone https://github.com/opencadc-metadata-curation/caom2pipe.git && \
  git pull origin master &&  pip install ./caom2pipe
  
RUN git clone https://github.com/opencadc-metadata-curation/neossat2caom2.git && \
  cp ./neossat2caom2/scripts/config.yml / && \
  cp ./neossat2caom2/scripts/state.yml / && \
  cp ./neossat2caom2/scripts/docker-entrypoint.sh / && \
  pip install ./neossat2caom2

RUN apk --no-cache del git \
  g++

ENTRYPOINT ["/docker-entrypoint.sh"]

