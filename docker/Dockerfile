FROM hysds/pge-base:latest
MAINTAINER jlinick "Justin.P.Linick@jpl.nasa.gov"
LABEL description="USGS NEIC query & evaluation base image"

ARG git_oauth_token
ARG id
ARG gid

#Change UID and GID from build
USER root
RUN set -ex && \
  usermod -u ${id} ops && \
  groupmod -g ${gid} ops && \
  chown -R ops:ops /home/ops/ /data && \
  usermod -g ${gid} ops

#install shapely for client side filtering
RUN pip install shapely

USER ops
# copy packages
COPY . /home/ops/verdi/ops/usgs_neic
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/usgs_neic

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
