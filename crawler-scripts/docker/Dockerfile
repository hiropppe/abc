FROM panubo/s3fs

WORKDIR /root

RUN apt-get update \
 && apt-get install -y \
        curl wget vim git procps tree \
        build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev \
        openjdk-8-jdk 

RUN curl -O https://www.python.org/ftp/python/3.7.6/Python-3.7.6.tar.xz \
 && tar -xf Python-3.7.6.tar.xz \
 && cd Python-3.7.6 \
 && ./configure --enable-optimizations \
 && make -j8 \
 && make install

RUN pip3 install --upgrade pip
RUN pip --no-cache-dir install \
        ipykernel \
        snakemake \
        tldextract

RUN ln -s /usr/local/bin/python3 /usr/local/bin/python

RUN wget http://builds.archive.org/maven2/org/archive/heritrix/heritrix/3.4.0-SNAPSHOT/heritrix-3.4.0-20190828.200101-25-dist.tar.gz \
 && tar xzf heritrix-3.4.0-20190828.200101-25-dist.tar.gz \
 && cd heritrix-3.4.0-SNAPSHOT

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HERITRIX_HOME /root/heritrix-3.4.0-SNAPSHOT
ENV JAVA_OPTS -Xmx10G
