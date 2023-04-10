FROM ubuntu:20.04

# Set timezone
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Setting the locale
ENV LC_ALL=en_US.UTF-8
RUN apt-get update && apt-get install --no-install-recommends -y locales
RUN sed -i "/$LC_ALL/s/^# //g" /etc/locale.gen
RUN dpkg-reconfigure --frontend=noninteractive locales
RUN update-locale LANG=${LC_ALL}

# Install required system packages
RUN apt-get -q -y update \
    && DEBIAN_FRONTEND=noninteractive apt-get -q -y upgrade \
    && apt-get -q -y install \
        python3 \
        pip \
        virtualenv \
        postgresql-client \
        unzip \
        wget \
    && apt-get -q clean \
    && rm -rf /var/lib/apt/lists/*

# Define environment variables
ENV DATAPUSHER_HOME=/usr/lib/ckan/datapusher
ENV DATAPUSHER_CODE=$DATAPUSHER_HOME/code
ENV DATAPUSHER_VENV=$DATAPUSHER_HOME/venv
ENV DATAPUSHER_CONFIG=/etc/ckan/datapusher

# Create ckan user
RUN useradd -r -u 900 -m -c "ckan account" -d $DATAPUSHER_HOME -s /bin/false ckan

# Install qsv
ENV QSV_RELEASE=0.99.0
ENV QSV_ARCHIVE=qsv-$QSV_RELEASE-x86_64-unknown-linux-gnu.zip
RUN cd /tmp && \
    wget https://github.com/jqnatividad/qsv/releases/download/$QSV_RELEASE/$QSV_ARCHIVE && \
    unzip $QSV_ARCHIVE && mv qsvdp /usr/local/bin/ && rm $QSV_ARCHIVE

# Setup virtual environment for CKAN
RUN mkdir -p $DATAPUSHER_CONFIG && \
    virtualenv $DATAPUSHER_VENV && \
    ln -s $DATAPUSHER_VENV/bin/pip3 /usr/local/bin/ckan-pip3 && \
    ln -s $DATAPUSHER_VENV/bin/ckan /usr/local/bin/ckan

# Virtual environment binaries/scripts to be used first
ENV PATH=${DATAPUSHER_VENV}/bin:${PATH}

# install the dependencies
RUN ckan-pip3 install -U pip && \
    CPUCOUNT=1 ckan-pip3 install --upgrade --no-cache-dir uwsgi psycopg2-binary

# Copy the ckan code to the image
COPY . $DATAPUSHER_CODE
RUN cp $DATAPUSHER_CODE/container/initialize-and-start.sh / && \
    chmod +x /initialize-and-start.sh

# install datapusher-plus
RUN ckan-pip3 install -e $DATAPUSHER_CODE

# Set ownership of directories
RUN chown -R ckan:ckan $DATAPUSHER_HOME $DATAPUSHER_CONFIG

USER ckan
EXPOSE 8800

CMD ["/initialize-and-start.sh"]
