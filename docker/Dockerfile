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
    apt-get install -y software-properties-common build-essential cmake pkg-config autoconf \
        vim wget bzip2 unzip less tree bc \
        libboost-all-dev liblzma-dev liblbfgs-dev \
        openjdk-8-jdk pdftohtml \
        zlib1g-dev libbz2-dev \
        ca-certificates libglib2.0-0 libxext6 libsm6 libxrender1 git mercurial subversion \
        language-pack-ja-base && \
    apt-get clean

RUN wget --quiet https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh -O ~/anaconda.sh && \
    /bin/bash ~/anaconda.sh -b -p /opt/conda && \
    rm ~/anaconda.sh && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete && \
    /opt/conda/bin/conda clean -afy

RUN /opt/conda/bin/conda  install -y pytorch==1.2.0 torchvision==0.4.0 faiss-gpu==1.6.0 cudatoolkit=10.0 -c pytorch

RUN /opt/conda/bin/pip install setuptools
RUN /opt/conda/bin/pip install ipykernel jedi flake8 autopep8 tqdm \
        Cython==0.29.6 \
        transliterate==1.10.2 \
        jieba==0.39 \
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
        neologdn \
        jpype1==0.7.0 \
        alcazar==0.5.1 \
        selectolax==0.2.1 \
        mmh3==2.5.1 \
        python-pdfextract==1.0.1.2 \
        git+https://github.com/slaveofcode/boilerpipe3.git \
        pycld2==0.41 \
        func_timeout==4.3.5 \
        attrdict

WORKDIR /root

RUN git clone --depth 1 https://github.com/taku910/mecab && \
    cd mecab/mecab && \
    ./configure --enable-utf8-only && \
    make && \
    make check && \
    make install && \
    /opt/conda/bin/pip install --no-cache-dir mecab-python3 && \
    ldconfig && \
    cd ../mecab-ipadic && \
    ./configure --with-charset=utf8 && \
    make && \
    make install

RUN git clone https://github.com/moses-smt/mosesdecoder.git && \
    cd mosesdecoder && ./bjam -j4

RUN git clone https://github.com/moses-smt/mgiza.git && \
    cd mgiza/mgizapp && git checkout 3dd8ef6 && cmake . && make && \
    cp ../experimental/alignment-enabled/MGIZA/scripts/merge_alignment.py ./bin/

RUN git clone https://github.com/bitextor/hunalign.git && \
    cd hunalign/src/hunalign && git checkout 8235ea2 && make

#RUN git clone https://github.com/kpu/preprocess.git && \
#    cd preprocess && git checkout 2d78e88 && cmake . && make -j4

RUN git clone https://github.com/jonsafari/clustercat.git && \
    cd clustercat && git checkout b19f171 && make -j4

RUN git clone https://github.com/kpu/kenlm && \
    cd kenlm && mkdir build && cd build && cmake .. -DKENLM_MAX_ORDER=7 && make -j all install

RUN git clone https://github.com/bitextor/bicleaner.git && \
    cd bicleaner && git checkout bicleaner-0.13 && python setup.py install && \
    /opt/conda/bin/pip install mosestokenizer toolwrapper regex

RUN git clone https://github.com/bitextor/bifixer.git && \
    /opt/conda/bin/pip install unidecode xxhash

RUN git clone https://github.com/hainan-xv/zipporah.git && \
    cd zipporah && ./make.sh

RUN git clone https://github.com/clab/fast_align.git && \
    cd fast_align && mkdir build && cd build && cmake .. && make

ENV PATH /opt/conda/bin:$PATH
RUN git clone https://github.com/hiropppe/strand-aligner.git && \
    cd strand-aligner && make && make install

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v3.9.1/protobuf-all-3.9.1.tar.gz && \
    tar xzf protobuf-all-3.9.1.tar.gz && \
    cd protobuf-3.9.1 && \
    ./configure && \
    make && \
    make check && \
    make install && \
    ldconfig && \
    pip install git+https://github.com/iamthebot/cld3

RUN git clone https://github.com/facebookresearch/fastText.git && \
    cd fastText && \
    make && \
    python setup.py install && \
    mkdir model && wget -O wget -O model/lid.176.bin https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin

#curl -fsSL https://github.com/rbenv/rbenv-installer/raw/master/bin/rbenv-installer | bash
#rbenv init
#echo 'export PATH=/root/.rbenv/bin:$PATH' >> ~/.bashrc
#echo 'eval "$(rbenv init -)"' >> ~/.bashrc
#apt-get install -y libssl-dev libreadline-dev
#rbenv install 2.6.5
#rbenv rehash
#gem install rbenv-rehash
#rbenv global 2.6.5
#gem install pragmatic_segmenter

RUN git clone https://github.com/facebookresearch/LASER.git
##  RUN git checkout 311b25e  # commit hash when tested
ENV LASER /root/LASER
RUN echo "export LASER=$LASER" >> ~/.bashrc

WORKDIR $LASER
#RUN /bin/bash ./install_models.sh
#RUN /bin/bash ./install_external_tools.sh
RUN mkdir -p ./tools-external/mecab/bin && \
    ln -s /usr/local/bin/mecab /root/LASER/tools-external/mecab/bin/mecab

WORKDIR /root

CMD [ "/bin/bash" ]
