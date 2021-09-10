# usage: ./abc/crawler-scripts/crawl.sh -j 10 -s /path/to/seed.gz

FROM ubuntu:18.04

ENV TZ Asia/Tokyo
ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y \
        git vim wget curl bzip2 unzip less tree \
        python3 python3-dev python3-pip \
        tzdata language-pack-ja-base && \
    apt-get clean

RUN pip3 install -U pip
RUN pip --no-cache-dir install \
        ipykernel \
        snakemake \
        tldextract \
        click \
        jedi flake8 autopep8

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV LANG ja_JP.UTF-8

WORKDIR /root

RUN git clone https://github.com/hiropppe/abc.git

CMD [ "/bin/bash" ]
