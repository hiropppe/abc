#!/usr/bin/env python3

import subprocess
import tldextract

from pathlib import Path


def system_check(cmd):
    subprocess.check_call(cmd, shell=True)


def get_lang_or_default_from_dict(scripts_dict, language):
    script = ""
    if language in scripts_dict:
        script = scripts_dict[language]
    elif "default" in scripts_dict:
        script = scripts_dict["default"]

    return script


def create_domainkey2hosts(hosts):
    ret={}
    for host in hosts:
        # don't merge blog sites
        if host.find(".blogspot.") >= 0 or host.find(".wordpress.") >= 0:
           key = host
        else:
           key = tldextract.extract(host).domain

        if key not in ret:
            ret[key]=[]
        ret[key].append(host)
        #print("subdomain", key, host)
    return ret


def filter_tld(tlds):
    filtered_tlds={}
    if os.path.isfile(f"{PERMANENT_DIR:}/domains.gz"):
        with open_gzip_or_plain(f"{PERMANENT_DIR}/domains.gz") as f:
            for tld in f:
                tld=tld.strip()
                filtered_tlds[tld]=tlds[tld]
        return filtered_tlds
    else:
        return tlds


def get_domain_hosts(wildcards):
    output=[]
    for h in domainkey2hosts[wildcards.target]:
        output.append(f'{DATA_DIR:}/warc/{h:s}/{CRAWLER:s}.warc.gz')
    return output


LANG1 = config["lang1"]
LANG2 = config["lang2"]

PERMANENT_DIR = Path(config["permanentDir"])
TRANSIENT_DIR = Path(config["transientDir"])
DATA_DIR = Path(config["dataDir"])

TMPDIR=config.get("temp", config["transientDir"])
shell("mkdir -p "+TMPDIR)

PROFILING=""

if "crawler" in config:
    CRAWLER = config["crawler"] 
else:
    CRAWLER = "wget"

PPROC = "w2p"
FILES=["plain_text.xz", "deboilerplate_html.xz", "normalized_html.xz", "mime.xz", "url.xz"]

if "preprocessLangs" in config and config["preprocessLangs"]:
    PPROCLANGSOPT = "--langs " + config['preprocessLangs']
    PPROCLANGS = config['preprocessLangs'].split(',')
else:
    PPROCLANGSOPT = "--langs " + LANG1 + "," + LANG2
    PPROCLANGS = [LANG1, LANG2]

if "targetLangs" in config and config["targetLangs"]:
    TARGETLANGSOPT = "--langs " + config['targetLangs']
    TARGETLANGS = config['targetLangs'].split(',')
else:
    TARGETLANGSOPT = "--langs " + LANG2
    TARGETLANGS = [LANG2]

if "ftfy" in config and not config["ftfy"]:
    FTFY = ""
else:
    FTFY = "--ftfy"

if "cleanHTML" in config and config["cleanHTML"]:
    CLEANHTML = "--cleanhtml"
else:
    CLEANHTML = ""

if "langId" in config:
    LANGID = config["langId"]
else:
    LANGID = "cld2"

if "boilerpipeCleaning" in config and config["boilerpipeCleaning"] is True:
    BOILERPIPE_CLEANING = '--boilerpipe'
else:
    BOILERPIPE_CLEANING = ''

if "pdf-converter" in config and config["pdf-converter"] == "pdf-extract":
    USE_PDF_EXTRACT = "--pdfextract"
else:
    USE_PDF_EXTRACT = ""

if "plainTextHashes" in config:
    PLAINTEXTHASHES = "--input_hash "+config["plainTextHashes"]
else:
    PLAINTEXTHASHES = ""

SENTTOKS = config["sentenceSplitters"]
SENTTOK1 = get_lang_or_default_from_dict(config["sentenceSplitters"], LANG1)
SENTTOK2 = get_lang_or_default_from_dict(config["sentenceSplitters"], LANG2)
if not SENTTOK1 or not SENTTOK2:
    sys.stderr.write("Sentence splitters for LANG1 and LANG2 are mandatory\n")
    exit(1)

WORDTOKS = config["wordTokenizers"]
WORDTOK1 = get_lang_or_default_from_dict(config["wordTokenizers"], LANG1)
WORDTOK2 = get_lang_or_default_from_dict(config["wordTokenizers"], LANG2)

if not WORDTOK1 or not WORDTOK2:
    sys.stderr.write("Word tokenizers for LANG1 and LANG2 are mandatory\n")
    exit(1)

if "morphologicalAnalysers" in config:
    MORPHTOKS = config["morphologicalAnalysers"]
    MORPHTOK1 = get_lang_or_default_from_dict(config["morphologicalAnalysers"], LANG1)
    MORPHTOK2 = get_lang_or_default_from_dict(config["morphologicalAnalysers"], LANG2)
else:
    MORPHTOKS = ""
    MORPHTOK1, MORPHTOK2 = "", ""

PARSER = config.get("parser", "")

DALIGN = config.get("documentAligner", "URL").lower()
PALIGN = config.get("paragraphAligner", "STRAND").lower()

# [SETUP TARGET HOSTS]

warc_path = DATA_DIR / "warc"
crawled_hosts = set([d.name for d in warc_path.iterdir() if (d / "{:s}.warc.gz".format(CRAWLER)).exists()])
print(f"read hosts from warc dir={len(crawled_hosts):d}")

input_hosts = None
if "hosts" in config:
    input_hosts = set(config["hosts"])
elif "hostsFile" in config:
    with gzip.open(config["hostsFile"], "rt") as f:
        input_hosts = set(f.read().splitlines())

if input_hosts:
    if config.get("incremental_execution", False):
        hosts = input_hosts - crawler_hosts
    else:
        hosts = input_hosts
else:
    hosts = crawled_hosts


domainkey2hosts = create_domainkey2hosts(hosts)

# If file domains.gz exists in the permanent directory, the dictionary domainKey2Hosts is filtered to contain only those TLD in this file
domainkey2hosts = filter_tld(domainkey2hosts)
print(domainkey2hosts)

# ================================== START SNAKEMAKE================================ #
OUTPUT = []
PPROC_OUTPUT = []
DALIGN_OUTPUT = []
PALIGN_OUTPUT = []

TASK_LIST = config["task"]
print(TASK_LIST)

if "concat" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        OUTPUT.append(f"{DATA_DIR:}/preprocess/{tld:s}/concat.warc.gz")

if "preprocessing" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        for lang in PPROCLANGS:
            for file in FILES:
                PPROC_OUTPUT.append(f"{DATA_DIR:}/preprocess/{tld:s}/{PPROC:s}/bitextorlang/{lang:s}/{file:s}")
            PPROC_OUTPUT.append(f"{DATA_DIR:}/preprocess/{tld:s}/{PPROC:s}/bitextorlang/{lang:s}/plain_tokenized.xz")

if "align-document" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        DALIGN_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext.{DALIGN}.xz")

if "align-paragraph" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        PALIGN_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext.{DALIGN}.{PALIGN}.ann")
        PALIGN_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext.{DALIGN}.{PALIGN}")


print(OUTPUT)
print(PPROC_OUTPUT)
print(DALIGN_OUTPUT)
print(PALIGN_OUTPUT)

rule all:
    input:
        expand("{target}", target=OUTPUT),
        expand("{target}", target=PPROC_OUTPUT),
        expand("{target}", target=DALIGN_OUTPUT),
        expand("{target}", target=PALIGN_OUTPUT)

# ================================== PREPROCESSING ====================================== #

rule concat_subdomains:
    input:
        get_domain_hosts
    output:
        f"{DATA_DIR}/preprocess/{{target}}/concat.warc.gz"
    priority: 9
    run:
        assert(len(input))
        if len(input) == 1:
            cmd = "ln -sfn {input} {output}; "
        else:
            cmd = 'cat {input} > {output}; '
        shell(cmd)


rule warc2preprocess:
    input:
        f'{DATA_DIR}/preprocess/{{domain}}/concat.warc.gz'
    output:
        hash = f'{DATA_DIR}/preprocess/{{domain}}/w2p/bitextorlang/plain_text_hashes.xz',
        files = expand('{data}/preprocess/{{domain}}/w2p/bitextorlang/{lang}/{file}', data=DATA_DIR, lang=PPROCLANGS, file=FILES)
    params:
        folder = f'{DATA_DIR}/preprocess/{{domain}}/w2p/bitextorlang'
    priority: 8
    threads: 2
    shell:
        'mkdir -p {params.folder};'
        '{PROFILING} ./scripts/pcm-warc2htmlwarc.py {CLEANHTML} {FTFY} --input {input} {USE_PDF_EXTRACT} '
        '| {PROFILING} nice ionice -c 3 ./scripts/pcm-warc2preprocess.py --input - {PPROCLANGSOPT} --lang1 {LANG1} --lang2 {LANG2} {BOILERPIPE_CLEANING} --langid {LANGID} --output-dir {params.folder} --output_hash {output.hash} {PLAINTEXTHASHES} {PARSER}; '
        'for lang in {PPROCLANGS}; do '
        '  if [ ! -f {params.folder}/$lang/plain_text.xz ]; then >&2 '
        '    echo "WARNING: no \'$lang\' data found in {wildcards.domain}. Creating empty files instead";'
        '    mkdir -p {params.folder}/$lang;'
        '    touch {params.folder}/$lang/plain_text {params.folder}/$lang/mime {params.folder}/$lang/url {params.folder}/$lang/normalized_html {params.folder}/$lang/deboilerplate_html ;'
        '    xz {params.folder}/$lang/*;'
        '  fi ; '
        'done'

rule tokenize:
    input: '{dir}/{lang}/plain_text.xz'
    params:
        splitter = lambda wildcards: get_lang_or_default_from_dict(SENTTOKS, wildcards.lang),
        tokenizer = lambda wildcards: get_lang_or_default_from_dict(WORDTOKS, wildcards.lang),
        lemmatizer = lambda wildcards: get_lang_or_default_from_dict(MORPHTOKS, wildcards.lang)
    output:
        '{dir}/{lang}/plain_tokenized.xz'
    shell:
        '{PROFILING} ./scripts/pcm-tokenize.py --text {input} --sentence-splitter "{params.splitter}" --word-tokenizer "{params.tokenizer}" --morph-analyser "{params.lemmatizer}" | xz -c > {output};'


# ================================= SIMPLE URL-BASED DOCUMENT ALIGNMENT ================================== #

rule align_doc_by_url:
    input:
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/plain_text.xz',
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/plain_text.xz',
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/url.xz',
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/url.xz',
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext.url.xz'
    shell:
        '{PROFILING} ./scripts/pcm-align-documents-by-url.py --lang1 {LANG1} --lang2 {LANG2} --text1 {input[0]} --text2 {input[1]} --url1 {input[2]} --url2 {input[3]} | xz -T 0 > {output}'

# ================================== STRAND ALIGNMENT ================================== #

rule strand_align:
    input:
        f"{TRANSIENT_DIR}/{{target}}/bitext.{DALIGN}.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/url.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/url.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/deboilerplate_html.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/deboilerplate_html.xz"
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext.{DALIGN}.strand.ann',
        f'{TRANSIENT_DIR}/{{target}}/bitext.{DALIGN}.strand'
    shell:
        'html_gz=$(mktemp "{TMPDIR}/html.docalign.XXXXXX.gz");'
        'strand_out=$(mktemp "{TMPDIR}/strand.align.XXXXXX");'
        'while IFS= read -r line;'
        'do '
        '  url1=$(echo "$line" | cut -f 1);'
        '  url2=$(echo "$line" | cut -f 2);'
        '  n1=$(xzcat -T 0 {input[1]} | grep -n "^$url1$" | head -n 1 | cut -d: -f1);'
        '  n2=$(xzcat -T 0 {input[2]} | grep -n "^$url2$" | head -n 1 | cut -d: -f1);'
        '  t1=$(xzcat -T 0 {input[3]} | sed -n "${{n1}}p");'
        '  t2=$(xzcat -T 0 {input[4]} | sed -n "${{n2}}p");'
        '  echo -e "k\t{LANG1}\t$url1\t$t1\t{LANG2}\t$url2\t$t2";'
        'done < <(xzcat -T 0 {input[0]}) | gzip -c > $html_gz;'
#        '{PROFILING} strand-align -i $html_gz -o $strand_out -ib64 -ob64 -ah;'
        '{PROFILING} strand-align -i $html_gz -o $strand_out -ib64 -ah;'
        'cat ${{strand_out}}.{LANG2}-{LANG1}.ann | awk -F$\'\t\' \'{{print $2"\\t"$1"\\t"$3"\\t"$4"\\t"$5"\\t"$7"\\t"$6}}\' > {output[0]};'
        'cat ${{strand_out}}.{LANG2}-{LANG1} | awk -F$\'\t\' \'{{print $3"\\t"$4"\\t"$1"\\t"$2"\\t"$5}}\' > {output[1]};'
        'rm $html_gz;'
        'rm $strand_out;'

# ================================== HUNALIGN FOR STRAND ALIGNMENT ================================== #

rule strand_align_hunalign:
    input:
        '{TRANSIENT_DIR}/{{target}}/bitext.{DALIGN}.strand.ann',
        '{TRANSIENT_DIR}/{{target}}/bitext.{DALIGN}.strand',
    output:
        '{TRANSIENT_DIR}/{{target}}/hunalign.{DALIGN}.strand.hunalign.xz'
    shell:
        ''
