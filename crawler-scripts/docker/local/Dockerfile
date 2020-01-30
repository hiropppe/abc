# usage: $HERITRIX_HOME/bin/heritrix -a admin:admin -b 0.0.0.0 -j /data/heritrix/jobs

FROM ubuntu:18.04

ENV TZ Asia/Tokyo
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y \
        git vim wget bzip2 unzip less tree \
        openjdk-8-jdk \
        python3 python3-dev python3-pip \
        tzdata language-pack-ja-base && \
    apt-get clean

RUN pip3 install -U pip
RUN pip --no-cache-dir install jedi flake8 autopep8

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV LANG ja_JP.UTF-8
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

WORKDIR /root

RUN wget http://builds.archive.org/maven2/org/archive/heritrix/heritrix/3.4.0-SNAPSHOT/heritrix-3.4.0-20190828.200101-25-dist.tar.gz \
 && tar xzf heritrix-3.4.0-20190828.200101-25-dist.tar.gz \
 && cd heritrix-3.4.0-SNAPSHOT

ENV HERITRIX_HOME /root/heritrix-3.4.0-SNAPSHOT
ENV JAVA_OPTS -Xmx10G

CMD [ "/bin/bash" ]
