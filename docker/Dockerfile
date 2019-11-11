FROM nvidia/cuda:10.0-cudnn7-devel-ubuntu16.04

RUN sed -i.bak -e "s%http://archive.ubuntu.com/ubuntu/%http://ftp.jaist.ac.jp/pub/Linux/ubuntu/%g" /etc/apt/sources.list

ENV TZ Asia/Tokyo

RUN echo '# LANG' >> ~/.bashrc \
 && echo 'export LANG=ja_JP.UTF-8' >> ~/.bashrc

RUN apt-get update \
  && apt-get install -y tzdata \
  && rm -rf /var/lib/apt/lists/* \
  && echo "${TZ}" > /etc/timezone \
  && rm /etc/localtime \
  && ln -s /usr/share/zoneinfo/Asia/Tokyo /etc/localtime \
  && dpkg-reconfigure -f noninteractive tzdata

RUN apt-get update && \
    apt-get install -y software-properties-common build-essential cmake pkg-config \
        vim wget bzip2 unzip git less tree \
        libboost-all-dev liblzma-dev liblbfgs-dev \
        openjdk-8-jdk pdftohtml \
        language-pack-ja-base && \
    apt-get clean

RUN add-apt-repository -y ppa:jonathonf/python-3.6 \
 && apt-get update \
 && apt-get install -y python3.6 python3.6-dev \
 && wget https://bootstrap.pypa.io/get-pip.py \
 && python3.6 get-pip.py \
 && ln -fs /usr/bin/python3.6 /usr/bin/python3 \
 && ln -fs /usr/bin/python3.6 /usr/bin/python

RUN pip install -U pip
RUN pip install --no-cache-dir -U setuptools
RUN pip --no-cache-dir install \
        jedi \
        flake8 \
        autopep8 \
        Cython==0.29.14 \
        Click==7.0 \
        beautifulsoup4==4.8.1 \
        lxml==4.4.1 \
        tldextract==2.2.2 \
        scipy==1.3.2 \
        nltk==3.4.5 \
        snakemake==5.7.1 \
        warcio==1.7.1 \
        cchardet==2.1.4 \
        python-magic==0.4.15 \
        ftfy==5.6 \
        jpype1==0.7.0 \
        alcazar==0.5.1 \
        selectolax==0.2.1 \
        mmh3==2.5.1 \
        python-pdfextract==1.0.1.2 \
        git+https://github.com/slaveofcode/boilerpipe3.git \
        pycld2==0.41 \
        ipykernel


WORKDIR /root

RUN git clone --depth 1 https://github.com/taku910/mecab && \
    cd mecab/mecab && \
    ./configure --enable-utf8-only && \
    make && \
    make check && \
    make install && \
    pip install --no-cache-dir mecab-python3 && \
    ldconfig && \
    cd ../mecab-ipadic && \
    ./configure --with-charset=utf8 && \
    make && \
    make install

RUN git clone https://github.com/moses-smt/mgiza.git && \
    cd mgiza/mgizapp && git checkout 3dd8ef6 && cmake . && make

RUN git clone https://github.com/bitextor/hunalign.git && \
    cd hunalign/src/hunalign && git checkout 8235ea2 && make

RUN git clone https://github.com/kpu/preprocess.git && \
    cd preprocess && git checkout 2d78e88 && cmake . && make -j4

## additional installation log

#RUN git clone https://github.com/hiropppe/strand-aligner.git && \
#    cd strand-aligner/maxend && make && python setup.py install && \
#    cd ../strand && make && python setup.py develop

CMD [ "/bin/bash" ]
