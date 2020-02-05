#!/usr/bin/env python3
import os
import tldextract

from pathlib import Path
from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider

os.environ['AWS_ACCESS_KEY_ID'] = ''
os.environ['AWS_SECRET_ACCESS_KEY'] = ''
os.environ['AWS_DEFAULT_REGION'] = ''
S3 = S3RemoteProvider()

def create_domainkey2hosts(hosts):
    ret = {}
    for host in hosts:
        key = tldextract.extract(host).domain
        if key not in ret:
            ret[key] = []
        ret[key].append(host)
    return ret


def get_domain_hosts(wildcards):
    output = []
    for host in domainkey2hosts[wildcards.target]:
        output.append(S3.remote(f'{DATA_BUCKET}/warc/{host}/{CRAWLER}.warc.gz'))
    return output


TMP_DIR = "/tmp"

DATA_BUCKET = "polyglot-warc-sample"

PERMANENT_DIR = "bitext-mining-test/permanent/bitextor-output"
TRANSIENT_DIR = "bitext-mining-test/transient"

PROFILING = "time"

CRAWLER = "heritrix"

TASK_LIST = ["concat",]

# ================================== DETERMINE TARGET HOSTS ================================ #
# warc_path = DATA_BUCKET + "/warc"
# crawled_hosts = set([d.name for d in warc_path.iterdir() if (d / "{:s}.warc.gz".format(CRAWLER)).exists()])
crawled_hosts = set()
print(f"read hosts from warc dir={len(crawled_hosts):d}")

input_hosts = set(["ahatoro.com", "okinawa2go.jp", "en.okinawa2go.jp"])
#input_hosts = None
#if "hosts" in config:
#    input_hosts = set(config["hosts"])
#elif "hostsFile" in config:
#    with gzip.open(config["hostsFile"], "rt") as f:
#        input_hosts = set(f.read().splitlines())

if input_hosts:
    if config.get("onlyNewHosts", True):
        hosts = input_hosts - crawled_hosts
    else:
        hosts = input_hosts

    domainkey2hosts = create_domainkey2hosts(hosts)
    mine_domain = list(domainkey2hosts.keys())
else:
    hosts = crawled_hosts
    domainkey2hosts = create_domainkey2hosts(hosts)

    if len(TASK_LIST) == 1 and TASK_LIST[0] == "finish":
        mine_domain = [d.name for d in TRANSIENT_DIR.iterdir() if (d / f"bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}{FILTER_SUFFIX}.xz").exists()]
    else:
        mine_domain = domainkey2hosts.keys()

# ================================== START SNAKEMAKE ================================ #

OUTPUT = []

if "concat" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        OUTPUT.append(S3.remote(f"preprocess/{tld}/concat.warc.gz"))

rule all:
    input:
        expand("{target}", target=OUTPUT),

# ================================== PREPROCESSING ====================================== #

rule concat_subdomains:
    input:
        get_domain_hosts
    output:
        S3.remote(f"{DATA_BUCKET}/preprocess/{{target}}/concat.warc.gz")
    shell:
        "cat {input} > {output}"


rule warc2preprocess:
    input:
        S3.remote(f"{DATA_BUCKET}/preprocess/{{domain}}/concat.warc.gz")
    output:
        hash = f"{DATA_BUCKET}/preprocess/{{domain}}/w2p/bitextorlang/plain_text_hashes.xz",
        files = expand('{data}/preprocess/{{domain}}/w2p/bitextorlang/{lang}/{file}', data=DATA_DIR, lang=PPROCLANGS, file=FILES)
    params:
        folder = f'{DATA_DIR}/preprocess/{{domain}}/w2p/bitextorlang'
    shell:
    shell:
        'mkdir -p {params.folder};'
        './scripts/warc2htmlwarc.py {CLEANHTML} {FTFY} --input {input} {USE_PDF_EXTRACT} '
        '| ./scripts/warc2preprocess.py --input - {PPROCLANGSOPT} --lang1 {LANG1} --lang2 {LANG2} {BOILERPIPE_CLEANING} --langid {LANGID} --output-dir {params.folder} --output_hash {output.hash} {PLAINTEXTHASHES} {PARSER} {NEOLOGDN}; '
        'for lang in {PPROCLANGS}; do '
        '  if [ ! -f {params.folder}/$lang/plain_text.xz ]; then >&2 '
        '    echo "WARNING: no \'$lang\' data found in {wildcards.domain}. Creating empty files instead";'
        '    mkdir -p {params.folder}/$lang;'
        '    touch {params.folder}/$lang/plain_text {params.folder}/$lang/mime {params.folder}/$lang/url {params.folder}/$lang/normalized_html {params.folder}/$lang/deboilerplate_html ;'
        '    xz -f {params.folder}/$lang/plain_text {params.folder}/$lang/mime {params.folder}/$lang/url {params.folder}/$lang/normalized_html {params.folder}/$lang/deboilerplate_html ;'
        '  fi ; '
        'done'
