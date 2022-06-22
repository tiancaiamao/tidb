FROM buildpack-deps:focal as base

COPY pd-server /bin/
COPY tikv-server /bin/
COPY start.sh /

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

COPY tikv.conf /etc/supervisor/conf.d/
COPY pd.conf /etc/supervisor/conf.d/
COPY tikv.toml /conf/tikv.toml

FROM base AS branch-version-1