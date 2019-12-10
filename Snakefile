#!/usr/bin/env python3

import gzip
import lzma
import subprocess
import shutil
import tldextract

from func_timeout import func_timeout, FunctionTimedOut
from pathlib import Path
from toolwrapper import ToolWrapper


def get_lang_or_default_from_dict(scripts_dict, language):
    script = ""
    if language in scripts_dict:
        script = scripts_dict[language]
    elif "default" in scripts_dict:
        script = scripts_dict["default"]
    return script


def create_domainkey2hosts(hosts):
    ret = {}
    for host in hosts:
        # don't merge blog sites
        if host.find(".blogspot.") >= 0 or host.find(".wordpress.") >= 0:
            key = host
        else:
            key = tldextract.extract(host).domain

        if key not in ret:
            ret[key] = []
        ret[key].append(host)
        # print("subdomain", key, host)
    return ret


def get_domain_hosts(wildcards):
    output = []
    for h in domainkey2hosts[wildcards.target]:
        output.append(f'{DATA_DIR:}/warc/{h:s}/{CRAWLER:s}.warc.gz')
    return output


if config.get("laser_knn_gpu", False) and config.get("laser_enc_gpu", False):
    try:
        GPUs = int(subprocess.check_output("nvidia-smi | grep Default | wc -l", shell=True).split()[0])
    except: # noqa
        GPUs = 0
else:
    GPUs = 0

LANG1 = config["lang1"]
LANG2 = config["lang2"]

PERMANENT_DIR = Path(config["permanentDir"])
TRANSIENT_DIR = Path(config["transientDir"])
DATA_DIR = Path(config["dataDir"])

TMP_DIR = config.get("temp", config["transientDir"])
shell("mkdir -p "+TMP_DIR)

PROFILING = "time"

MOSES = Path(config["moses"])
HUNALIGN = Path(config["hunalign"])
BICLEANER = Path(config["bicleaner"])
ZIPPORAH = Path(config["zipporah"])

CRAWLER = config.get("crawler", "wget")

PPROC = "w2p"
FILES = ["plain_text.xz", "deboilerplate_html.xz", "normalized_html.xz", "mime.xz", "url.xz"]

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

if "neologdn" in config and not config["neologdn"]:
    NEOLOGDN = ""
else:
    NEOLOGDN = "--neologdn"

if "pdf-converter" in config and config["pdf-converter"] == "pdf-extract":
    USE_PDF_EXTRACT = "--pdfextract"
else:
    USE_PDF_EXTRACT = ""

if "plainTextHashes" in config:
    PLAINTEXTHASHES = "--input_hash "+config["plainTextHashes"]
else:
    PLAINTEXTHASHES = ""

if config.get("deduped", True):
    DEDUP = '--dedup "seg1,seg2"'
    FILTER_SORT = f"LC_ALL=C sort -t$'\\t' -k3,4 -T {TMP_DIR} --compress-program=gzip |"
else:
    DEDUP = ""
    FILTER_SORT = ""

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
if PARSER:
    PARSER = "--parser " + PARSER

DALIGN = config.get("documentAligner", "URL").upper()
if DALIGN == "URL":
    DALIGN_SUFFIX = ".url"
else:
    DALIGN_SUFFIX = ""

PALIGN = config.get("paragraphAligner", "").upper()
if PALIGN == "STRAND":
    PALIGN_SUFFIX = ".srd"
    STRAND_THRESHOLD = config.get("strandThreshold", 0.01)
    STRAND_DP_THRESHOLD = config.get("strandDpThreshold", 0.7)
else:
    PALIGN_SUFFIX = ""

SALIGN = config.get("sentenceAligner", "HUNALIGN").upper()
if SALIGN == "HUNALIGN":
    SALIGN_SUFFIX = ".hun"
    HUNALIGN_DIC = config.get("hunalignDic")
    MIN_QUALITY = config.get("hunalignThreshold", 0.0)
elif SALIGN == "LASER":
    SALIGN_SUFFIX = ".lsr"
    LASER_ENCODER = config["laser_encoder"]
    LASER_BPE_CODES = config["laser_bpe_codes"]
    LASER_ENC_PROC = "--enc_gpu" if config.get("laser_enc_gpu", False) else "--enc_cpu"
    LASER_KNN_PROC = "--knn_gpu" if config.get("laser_knn_gpu", False) else "--knn_cpu"
    MIN_QUALITY = config.get("laser_threshold", 1.1)
else:
    SALIGN_SUFFIX = ""

if "bifixer" in config and config["bifixer"]:
    BIFIXER = "bifixer"
    BIFIXERFIELD = ",bifixerhash,bifixerscore"
    CACHEOPTIONS = '-k 6,7'
    FILTER_SORT = ""
    DEDUP = '--dedup "bifixerhash"'
else:
    BIFIXER = "segclean"
    BIFIXERFIELD = ""
    CACHEOPTIONS = '-k 3,4'

if "bifixerOptions" in config:
    BIFIXEROPTIONS = config["bifixerOptions"]
else:
    BIFIXEROPTIONS = "--aggressive_dedup"

if "lidetc" in config and config["lidetc"]:
    LIDETC_SUFFIX = ".lid"
else:
    LIDETC_SUFFIX = ""

LID = config["LID"].strip()
if ":" in LID:
    LID, LID_MODEL = LID.split(":")
    LID, LID_MODEL = LID.strip(), LID_MODEL.strip()
else:
    LID_MODEL = ""

FILTER = config.get("filter", "").upper()

if FILTER == "BICLEANER":
    FILTER_SUFFIX = ".bic"
    BICLEANER_CONFIG = config["bicleanerConfig"]
    BICLEANER_THRESHOLD = float(config.get("bicleanerThreshold", "0"))

elif FILTER == "ZIPPORAH":
    FILTER_SUFFIX = ".zip"
    ZIPO_DIR = config["zipporahDir"]
    ZIPO_CONFIG = config["zipporahConfig"]
    ZIPO_MODEL = config["zipporahModel"]
    ZIPO_DIC1 = config["zipporahDic1"]
    ZIPO_DIC2 = config["zipporahDic2"]
    ZIPO_VOCAB1 = config["zipporahVocab1"]
    ZIPO_VOCAB2 = config["zipporahVocab2"]
    ZIPO_LM1 = config["zipporahLM1"]
    ZIPO_LM2 = config["zipporahLM2"]
    ZIPO_THRESHOLD = config["zipporahThreshold"]
else:
    FILTER_SUFFIX = ""

MAX_LINES = int(config.get("maxlines", "-1"))

TASK_LIST = config["task"]
# print(TASK_LIST)

# ================================== DETERMINE TARGET HOSTS ================================ #
warc_path = DATA_DIR / "warc"
crawled_hosts = set([d.name for d in warc_path.iterdir() if (d / "{:s}.warc.gz".format(CRAWLER)).exists()])
# crawled_hosts = set()
print(f"read hosts from warc dir={len(crawled_hosts):d}")

input_hosts = None
if "hosts" in config:
    input_hosts = set(config["hosts"])
elif "hostsFile" in config:
    with gzip.open(config["hostsFile"], "rt") as f:
        input_hosts = set(f.read().splitlines())

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
PPROC_OUTPUT = []
DALIGN_OUTPUT = []
PALIGN_OUTPUT = []
SALIGN_OUTPUT = []
FILTER_OUTPUT = []

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
        DALIGN_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext{DALIGN_SUFFIX}.xz")

if "align-paragraph" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        PALIGN_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}.ann")
        PALIGN_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}")

if "align-sentence" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        SALIGN_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz")

if "filtering" in TASK_LIST:
    for tld in domainkey2hosts.keys():
        FILTER_OUTPUT.append(f"{TRANSIENT_DIR:}/{tld:s}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}{FILTER_SUFFIX}.xz")

if "finish" in TASK_LIST:
    if config.get("tmx", False):
        if config.get("deduped", True):
            OUTPUT.append(f"{PERMANENT_DIR}/{LANG1}-{LANG2}.deduped.tmx.xz")
            OUTPUT.append(f"{PERMANENT_DIR}/{LANG1}-{LANG2}.deduped.txt.xz")
        else:
            OUTPUT.append(f"{PERMANENT_DIR}/{LANG1}-{LANG2}.tmx.xz")
    else:
        OUTPUT.append(f"{PERMANENT_DIR}/{LANG1}-{LANG2}.raw.xz")

print(OUTPUT)
# print(PPROC_OUTPUT)
# print(DALIGN_OUTPUT)
# print(PALIGN_OUTPUT)
# print(SALIGN_OUTPUT)
# print(FILTER_OUTPUT)


rule all:
    input:
        expand("{target}", target=OUTPUT),
        expand("{target}", target=PPROC_OUTPUT),
        expand("{target}", target=DALIGN_OUTPUT),
        expand("{target}", target=PALIGN_OUTPUT),
        expand("{target}", target=SALIGN_OUTPUT),
        expand("{target}", target=FILTER_OUTPUT),

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
        '{PROFILING} ./scripts/warc2htmlwarc.py {CLEANHTML} {FTFY} --input {input} {USE_PDF_EXTRACT} '
        '| nice ionice -c 3 ./scripts/warc2preprocess.py --input - {PPROCLANGSOPT} --lang1 {LANG1} --lang2 {LANG2} {BOILERPIPE_CLEANING} --langid {LANGID} --output-dir {params.folder} --output_hash {output.hash} {PLAINTEXTHASHES} {PARSER} {NEOLOGDN}; '
        'for lang in {PPROCLANGS}; do '
        '  if [ ! -f {params.folder}/$lang/plain_text.xz ]; then >&2 '
        '    echo "WARNING: no \'$lang\' data found in {wildcards.domain}. Creating empty files instead";'
        '    mkdir -p {params.folder}/$lang;'
        '    touch {params.folder}/$lang/plain_text {params.folder}/$lang/mime {params.folder}/$lang/url {params.folder}/$lang/normalized_html {params.folder}/$lang/deboilerplate_html ;'
        '    xz -f {params.folder}/$lang/plain_text {params.folder}/$lang/mime {params.folder}/$lang/url {params.folder}/$lang/normalized_html {params.folder}/$lang/deboilerplate_html ;'
        '  fi ; '
        'done'

rule tokenize:
    input:
        '{dir}/{lang}/plain_text.xz'
    params:
        splitter = lambda wildcards: get_lang_or_default_from_dict(SENTTOKS, wildcards.lang),
        tokenizer = lambda wildcards: get_lang_or_default_from_dict(WORDTOKS, wildcards.lang),
        lemmatizer = lambda wildcards: get_lang_or_default_from_dict(MORPHTOKS, wildcards.lang)
    output:
        '{dir}/{lang}/plain_tokenized.xz'
    shell:
        '{PROFILING} ./scripts/tokenizer.py --text {input} --sentence-splitter "{params.splitter}" --word-tokenizer "{params.tokenizer}" --morph-analyser "{params.lemmatizer}" | xz -c > {output};'


# ================================= DOCUMENT ALIGNMENT (SIMPLE URL NORMALIZATION) ================================== #

rule align_doc_by_url:
    input:
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/plain_text.xz',
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/plain_text.xz',
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/url.xz',
        f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/url.xz',
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext.url.xz'
    shell:
        '{PROFILING} ./scripts/align_documents_by_url.py --lang1 {LANG1} --lang2 {LANG2} --text1 {input[0]} --text2 {input[1]} --url1 {input[2]} --url2 {input[3]} | xz -T 0 > {output}'

# ================================== SEGMENT ALIGNMENT (LAZER) ================================== #

rule prepare_laser_mine:
    input:
        f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/deboilerplate_html.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/deboilerplate_html.xz",
    output:
        f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.cc.{LANG1}",
        f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.cc.{LANG2}",
        f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.cc.offset",
    params:
        prefix = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}'
    shell:
        'html_gz=$(mktemp "{TMP_DIR}/laser_prepare.html.XXXXXX.gz");'

        'while IFS= read -r line;'
        'do '
        '  n1=$(echo "$line" | cut -f1);'
        '  n2=$(echo "$line" | cut -f2);'
        '  t1=$(xzcat -T 0 {input[1]} | sed -n "${{n1}}p");'
        '  t2=$(xzcat -T 0 {input[2]} | sed -n "${{n2}}p");'
        '  echo -e "$n1\t$n2\t$t1\t$t2";'
        'done < <(xzcat -T 0 {input[0]}) | gzip -c > $html_gz;'

        '{PROFILING} zcat $html_gz | ./scripts/prepare_laser_mine.py -p {params.prefix} -sl {LANG1} -tl {LANG2} -s1 "{SENTTOK1}" -s2 "{SENTTOK2}" --lid {LID} --lid_model {LID_MODEL};'

        'rm $html_gz;'

rule laser_mine:
    input:
        src = f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.cc.{LANG1}",
        tgt = f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.cc.{LANG2}",
        offset = f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.cc.offset",
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.lsr.ind.xz'
    shell:
        'gpid_dir=/tmp;'
        'gpu=-1;'

        'if [ ! {GPUs} -eq 0 ]; then'
#        '    if [ ! -s /tmp/gpus ]; then'
#        '        nvidia-smi -L | awk \'{{print$2}}\' | cut -c1 > /tmp/gpus;'
#        '    fi;'

        '    while true \n'
        '    do \n'
        '        while read gpu; \n'
        '        do \n'
        '            if [ -f ${{gpid_dir}}/gpu${{gpu}}.lock ]; then'
        '                continue;'
        '            fi;'
        '            use_rate=$(nvidia-smi -i $gpu | grep Default | awk \'{{print$13}}\');'
        '            if [ $use_rate = "0%" ]; then'
        '                echo "using GPU${{gpu}}"; '
        '                touch ${{gpid_dir}}/gpu${{gpu}}.lock;'
        '                break 2;'
        '            fi; '
        '        done < <(nvidia-smi -L | awk \'{{print$2}}\' | cut -c1); \n'
#        '        done < /tmp/gpus; \n'
        '        sleep 3;'
        '    done; \n'
        'fi;'

        '{PROFILING} CUDA_VISIBLE_DEVICES=$gpu ./scripts/laser_mine.py --src {input.src} --tgt {input.tgt} --offset {input.offset} --slang {LANG1} --tlang {LANG2}'
        '  --encoder {LASER_ENCODER} --bpe_codes {LASER_BPE_CODES} {LASER_ENC_PROC}'
        '  --unify --mode mine --retrieval max --margin ratio -k 4 --verbose {LASER_KNN_PROC} --output {output};'

        'if [ ! -f {output} ]; then'
        '    touch {TRANSIENT_DIR}/{wildcards.target}/bitext{DALIGN_SUFFIX}.lsr.ind;'
        '    xz {TRANSIENT_DIR}/{wildcards.target}/bitext{DALIGN_SUFFIX}.lsr.ind;'
        'fi;'

        'if [ -f ${{gpid_dir}}/gpu${{gpu}}.lock ]; then'
        '    rm -f ${{gpid_dir}}/gpu${{gpu}}.lock;'
        'fi;'

# ================================== SEGMENT ALIGNMENT (STRAND + HUNALIGN) ================================== #

rule strand_align:
    input:
        f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/url.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/url.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/deboilerplate_html.xz",
        f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/deboilerplate_html.xz"
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.srd.ann',
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.srd'
    shell:
        'html_gz=$(mktemp "{TMP_DIR}/html.docalign.XXXXXX.gz");'
        'strand_out=$(mktemp "{TMP_DIR}/strand.align.XXXXXX");'

        'while IFS= read -r line;'
        'do '
        '  n1=$(echo "$line" | cut -f1);'
        '  n2=$(echo "$line" | cut -f2);'
        '  url1=$(xzcat -T 0 {input[1]} | sed -n "${{n1}}p");'
        '  url2=$(xzcat -T 0 {input[2]} | sed -n "${{n2}}p");'
        '  t1=$(xzcat -T 0 {input[3]} | sed -n "${{n1}}p");'
        '  t2=$(xzcat -T 0 {input[4]} | sed -n "${{n2}}p");'
        '  echo -e "k\t{LANG1}\t$url1\t$t1\t{LANG2}\t$url2\t$t2";'
        'done < <(xzcat -T 0 {input[0]}) | gzip -c > $html_gz;'

        '{PROFILING} strand-align -i $html_gz -o $strand_out -ib64 -ah;'

        'if [ -f ${{strand_out}}.{LANG2}-{LANG1}.ann ]; then'
        '  cat ${{strand_out}}.{LANG2}-{LANG1}.ann | awk -F$\'\t\' \'{{print $2"\\t"$1"\\t"$3"\\t"$4"\\t"$5"\\t"$7"\\t"$6}}\' > {output[0]};'
        '  cat ${{strand_out}}.{LANG2}-{LANG1} | awk -F$\'\t\' \'{{print $3"\\t"$4"\\t"$1"\\t"$2"\\t"$5}}\' > {output[1]};'
        'else'
        '  touch {output[0]};'
        '  touch {output[1]};'
        'fi;'

        'rm $html_gz;'
        'rm $strand_out;'

rule stranded_hunalign:
    """ HUNALIGN with STRAND as input
    """
    input:
        ann = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}.ann',
        bitext = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}',
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.srd.hun.xz.temp'
    shell:
        '{PROFILING} ./scripts/hunalign_strand.py {input.ann} {input.bitext} -ha {HUNALIGN}/src/hunalign -dic {HUNALIGN_DIC} -s1 "{SENTTOK1}" -s2 "{SENTTOK2}" -w1 "{WORDTOK1}" -w2 "{WORDTOK2}" -t {TMP_DIR} --dp_threshould {STRAND_DP_THRESHOLD} --cost_threshould {STRAND_THRESHOLD} | xz -T 0 > {output}'

# ================================== SEGMENT ALIGNMENT (HUNALIGN) ================================== #

rule hunalign:
    input:
        docalign = f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.xz",
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.hun.ind.xz'
    shell:
        'xzcat -T 0 {input.docalign} | ./scripts/hunalign_old.py -d {HUNALIGN_DIC} -t {TMP_DIR} --lang1 {LANG1} --lang2 {LANG2} --hunalign-dir "{HUNALIGN}/src/hunalign" --sent-tokeniser_sl "{SENTTOK1}" --sent-tokeniser_tl "{SENTTOK2}" --word-tokeniser_sl "{WORDTOK1}" --word-tokeniser_tl "{WORDTOK2}" | xz -T 0 > {output};'

"""
rule prepare_hunalign:
    input:
        indices = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.xz',
        plain1 = f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/plain_text.xz',
        plain2 = f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/plain_text.xz',
        tok1 = f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/plain_tokenized.xz',
        tok2 = f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/plain_tokenized.xz',
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.full.xz'
    shell:
        'sorted=$(mktemp "{TMP_DIR}/docalign.sorted.XXXXXX");'
        'xzcat -T 0 {input.indices} | LC_ALL=C sort -nk1 > $sorted;'
        '{PROFILING} python3 ./scripts/build_docalign.py --indices $sorted --text1 {input.plain1} --text2 {input.plain2} --tokenized1 {input.tok1} --tokenized2 {input.tok2} | xz -T 0 -c > {output};'
        'rm $sorted'

rule hunalign:
    input:
        docalign = f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.full.xz",
    output:
        temp(f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.hun.ind.xz')
    shell:
        '{PROFILING} xzcat -T 0 {input.docalign} | ./scripts/hunalign.py -d {HUNALIGN_DIC} -t {TMP_DIR} --lang1 {LANG1} --lang2 {LANG2} --hunalign-dir {HUNALIGN}/src/hunalign --sent-tokeniser_sl "{SENTTOK1}" --sent-tokeniser_tl "{SENTTOK2}" | xz -T 0 > {output};'
"""
# ================================== POST SEGMENT ALIGNMENT ================================== #

rule indices2url:
    input:
        segalign = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.ind.xz',
        url1 = f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/url.xz',
        url2 = f'{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/url.xz'
    output:
        segalign = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz.temp'
    run:
        lang1_dict = {}
        lang2_dict = {}
        counter = 1
        with lzma.open(input.url1, "rt") as url_reader:
            for line in url_reader:
                lang1_dict[counter] = line.strip()
                counter = counter + 1
        counter = 1
        with lzma.open(input.url2, "rt") as url_reader:
            for line in url_reader:
                lang2_dict[counter] = line.strip()
                counter = counter + 1
        with lzma.open(input.segalign, "rt") as reader, lzma.open(output.segalign, "wt") as writer:
            for line in reader:
                fields = line.strip().split('\t')
                writer.write('{}\t{}\t{}\n'.format(lang1_dict[int(fields[0])], lang2_dict[int(fields[1])], "\t".join(fields[2:])))

rule clean_segment:
    input:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz.temp',
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz',
    shell:
        '{PROFILING} xzcat -T 0 -f {input} | ./scripts/clean_segment.py -q {MIN_QUALITY} -m {MAX_LINES} -s | xz -T 0 > {output}'

# ================================== CLEANING (FILTERING) ================================== #

rule bicleaner:
    input:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz'
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.bic.score.xz'
    shell:
        'scores=$(mktemp "{TMP_DIR}/bicleaner.scores.XXXXXX");'
        'slang=$(egrep "source_lang" {BICLEANER_CONFIG} | cut -d " " -f 2); '
        'if [ "$slang" == "{LANG1}" ]; then '
        '  xzcat -T 0 -f {input} | python3 {BICLEANER}/bicleaner/bicleaner_classifier_lite.py --score_only -q --threshold {BICLEANER_THRESHOLD} - - {BICLEANER_CONFIG} --scol 3 --tcol 4 > $scores; '
        'else '
        '  xzcat -T 0 -f {input} | python3 {BICLEANER}/bicleaner/bicleaner_classifier_lite.py --score-only -q --threshold {BICLEANER_THRESHOLD} - - {BICLEANER_CONFIG} --scol 4 --tcol 3 > $scores; '
        'fi;'
        'paste <(xzcat -T 0 {input}) $scores | xz -T 0 > {output};'
        'rm $scores;'

rule bicleaner_filter:
    input:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.bic.score.xz'
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.bic.xz'
    shell:
        '{PROFILING} xzcat -T 0 -f {input} | ./scripts/filter.py --threshold {BICLEANER_THRESHOLD} | xz -T 0 > {output}'

rule zipporah_trans_score:
    input:
        data = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz',
    output:
        trans1 = f"{TRANSIENT_DIR}/{{target}}/translation.{LANG1}-{LANG2}",
        trans2 = f"{TRANSIENT_DIR}/{{target}}/translation.{LANG2}-{LANG1}",
    shell:
        'if [ $(xzcat {input.data} | wc -l) -eq 0 ]; then '
        '    touch {output.trans1}; '
        '    touch {output.trans2}; '
        'else'
        '    tmpfolder={TRANSIENT_DIR}/{wildcards.target}/translation/;'
        '    mkdir -p $tmpfolder;'
        '    rm -rf $tmpfolder;'
        '    mkdir -p $tmpfolder;'

        '    xzcat -T 0 -f {input.data} | awk -F \'\t\' \'BEGIN{{OFS="\t"}} {{print ($3, $4)}}\' > $tmpfolder/pasted;'

        '    cat $tmpfolder/pasted | awk -F \'\t\' \'{{print $1}}\' | {WORDTOK1} > $tmpfolder/s.in;'
        '    cat $tmpfolder/pasted | awk -F \'\t\' \'{{print $2}}\' | {WORDTOK2} > $tmpfolder/s.out;'

        '    {ZIPPORAH}/scripts/generate-translation-scores.sh {ZIPO_CONFIG} $tmpfolder/s.in $tmpfolder/s.out {ZIPO_DIC1} $tmpfolder/out.f2e;'
        '    {ZIPPORAH}/scripts/generate-translation-scores.sh {ZIPO_CONFIG} $tmpfolder/s.out $tmpfolder/s.in {ZIPO_DIC2} $tmpfolder/out.e2f;'

        '    touch {output.trans1} {output.trans2};'
        '    rm {output.trans1} {output.trans2};'

        '    cat $tmpfolder/out.f2e >> {output.trans1};'
        '    cat $tmpfolder/out.e2f >> {output.trans2};'
        'fi;'

rule zipporah_lm_score:
    input:
        data = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz',
    params:
        vocab = lambda w: f'{ZIPO_DIR}/vocab.{w["lang"]}',
        lm = lambda w: f'{ZIPO_DIR}/lm.{w["lang"]}',
        col = lambda w: 3 if w["lang"] == LANG1 else 4,
        tok = lambda w: WORDTOK1 if w["lang"] == LANG1 else WORDTOK2
    output:
        ngram = f"{TRANSIENT_DIR}/{{target}}/ngram.{{lang, [a-z][a-z]}}",
    shell:
        'if [ $(xzcat {input.data} | wc -l) -eq 0 ]; then '
        '    touch {output.ngram}; '
        'else'
        '    map_unk=`tail -n 1 {params.vocab}`;'
        '    xzcat -T 0 -f {input.data} | cut -f{params.col} | {params.tok} | awk -v v={params.vocab} -v u=$map_unk \'BEGIN{{while((getline<v)>0) m[$1]=1;}}{{for(i=1;i<=NF;i++) {{w=$i; if(m[w] !=1) w=u; printf("%s ", w)}}; print""}}\' | {MOSES}/bin/query -v sentence {params.lm} | grep ^Total | awk \'{{print -$2}}\' > {TRANSIENT_DIR}/{wildcards.target}/ngram.total.{wildcards.lang};'
        # +1 because of the EOS symbol
        '    xzcat -T 0 -f {input.data} | cut -f{params.col} | {params.tok} | awk \'{{print NF + 1}}\' > {TRANSIENT_DIR}/{wildcards.target}/ngram.length.{wildcards.lang};'
        '    paste {TRANSIENT_DIR}/{wildcards.target}/ngram.total.{wildcards.lang} {TRANSIENT_DIR}/{wildcards.target}/ngram.length.{wildcards.lang} | awk \'{{print $1 / $2}}\' > {output.ngram};'
        'fi;'

rule zipporah_feats:
    input:
        trans1 = f"{TRANSIENT_DIR}/{{target}}/translation.{LANG1}-{LANG2}",
        trans2 = f"{TRANSIENT_DIR}/{{target}}/translation.{LANG2}-{LANG1}",
        ngram1 = f"{TRANSIENT_DIR}/{{target}}/ngram.{LANG1}",
        ngram2 = f"{TRANSIENT_DIR}/{{target}}/ngram.{LANG2}",
    output:
        feats = f"{TRANSIENT_DIR}/{{target}}/zipporah.feats",
    shell:
        'paste {input.trans1} {input.trans2} | awk \'{{print ($1) + ($2)}}\' > {TRANSIENT_DIR}/{wildcards.target}/translation.sum;'

        'paste {input.trans1} {input.trans2} {input.ngram1} {input.ngram2} | awk \'{{print ($1)+($2),"\t",($3)+($4)}}\' | awk \'{{a=$1/10;b=$2/10;print a^8,b^8}}\' | tee {output.feats} | awk \'{{print NF}}\' | uniq -c;'

rule zipporah:
    input:
        bitext = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.xz',
        X = f"{TRANSIENT_DIR}/{{target}}/zipporah.feats",
    output:
        y = f"{TRANSIENT_DIR}/{{target}}/zipporah.score",
        zipp = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.zip.score.xz'
    shell:
        'if [ ! -s {input.X} ]; then'
        '    touch {output.y};'
        '    touch {TRANSIENT_DIR}/{wildcards.target}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.zip.score;'
        '    xz {TRANSIENT_DIR}/{wildcards.target}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.zip.score;'
        'elif [ ! $(xzcat {input.bitext} | wc -l) -eq $(cat {input.X} | wc -l) ]; then'
        '    echo "input size mismatch!";'
        '    touch {output.y};'
        '    touch {TRANSIENT_DIR}/{wildcards.target}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.zip.score;'
        '    xz {TRANSIENT_DIR}/{wildcards.target}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.zip.score;'
        'else'
        '    python ./scripts/zipporah.py predict {input.X} {ZIPO_MODEL} {output.y};'
        '    paste <(xzcat -T 0 {input.bitext}) {output.y} | xz -T 0 > {output.zipp};'
        'fi;'

rule zipporah_filter:
    input:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.zip.score.xz'
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}.zip.xz'
    shell:
        '{PROFILING} xzcat -T 0 -f {input} | ./scripts/filter.py --threshold {ZIPO_THRESHOLD} | xz -T 0 > {output}'

# ================================== Finish  ================================== #

rule lidetc:
    input:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}{FILTER_SUFFIX}.xz'
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}{FILTER_SUFFIX}.lid.xz'
    params:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}{FILTER_SUFFIX}.lid-err.xz'
    shell:
        'xzcat -T 0 -f {input} | python3 ./scripts/lidetc.py --lid {LID} --lid_model {LID_MODEL} --lang1 {LANG1} --lang2 {LANG2} --tokenizer1 moses --tokenizer2 mecab --err_out {params} | xz -T 0 > {output}'

rule raw:
    input:
        expand(f"{TRANSIENT_DIR}/{{domain}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}{SALIGN_SUFFIX}{FILTER_SUFFIX}{LIDETC_SUFFIX}.xz", dir=TRANSIENT_DIR, domain=mine_domain)
    output:
        f"{PERMANENT_DIR}/{LANG1}-{LANG2}.raw.xz"
    run:
        with open(output[0], 'wb') as wfd:
            for f in input:
                with open(f, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd, 1024*1024*10)

rule tmx:
    input:
        f"{PERMANENT_DIR}/{{LANG1}}-{{LANG2}}.raw.xz"
    output:
        f"{PERMANENT_DIR}/{{LANG1}}-{{LANG2}}.tmx.xz"
    shell:
        "xzcat -T 0 -f {input} | ./scripts/build_tmx.py --lang1 {LANG1} --lang2 {LANG2} -c url1,url2,seg1,seg2 | xz -T 0 > {output.tmx}"

rule deduped_tmx:
    input:
        f"{PERMANENT_DIR}/{{LANG1}}-{{LANG2}}.raw.xz"
    output:
        tmx = f"{PERMANENT_DIR}/{{LANG1}}-{{LANG2}}.deduped.tmx.xz",
        txt = f"{PERMANENT_DIR}/{{LANG1}}-{{LANG2}}.deduped.txt.xz"
    shell:
        "xzcat -T 0 -f {input} | {FILTER_SORT} ./scripts/build_tmx.py --lang1 {LANG1} --lang2 {LANG2} -c url1,url2,seg1,seg2 {DEDUP} -f {output.txt} | xz -T 0 > {output.tmx}"
