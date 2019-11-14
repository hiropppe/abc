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

TMP_DIR = config.get("temp", config["transientDir"])
shell("mkdir -p "+TMP_DIR)

PROFILING = ""

MOSES = Path(config["moses"])
HUNALIGN = Path(config["hunalign"])
MGIZA = Path(config["mgiza"])

MKCLS = config.get("mkcls")
if MKCLS:
    MKCLS = Path(MKCLS)
else:
    MKCLS = MGIZA / "mgizapp" / "bin" / "mkcls"

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

DALIGN = config.get("documentAligner", "URL").upper()
if DALIGN == "URL":
    DALIGN_SUFFIX = ".url"
else:
    DALIGN_SUFFIX = ""

PALIGN = config.get("paragraphAligner", "").upper()
if PALIGN == "STRAND":
    PALIGN_SUFFIX = ".srd"
else:
    PALIGN_SUFFIX = ""

SALIGN = config.get("sentenceAligner", "HUNALIGN").upper()
if SALIGN == "HUNALIGN":
    SALIGN_SUFFIX = ".hun"
else:
    SALIGN_SUFFIX = ""

TRAIN_PREFIXES = config.get("initCorpusTrainPrefix")

PPROC_CORPUS_DIR = f"{TRANSIENT_DIR}/tempcorpuspreproc.{LANG1}-{LANG2}"
MGIZA_MODEL_DIR = f"{TRANSIENT_DIR}/tempgizamodel.{LANG1}-{LANG2}"

DIC = config.get("dic")
HUNALIGN_DIC = config.get("hunalignDic")


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
    if config.get("onlyNewHosts", True):
        hosts = input_hosts - crawled_hosts
    else:
        hosts = input_hosts
else:
    hosts = crawled_hosts

domainkey2hosts = create_domainkey2hosts(hosts)

# If file domains.gz exists in the permanent directory, the dictionary domainKey2Hosts is filtered to contain only those TLD in this file
domainkey2hosts = filter_tld(domainkey2hosts)
print(domainkey2hosts)

# ================================== START SNAKEMAKE================================ #
BUILD_OUTPUT = []
OUTPUT = []
PPROC_OUTPUT = []
DALIGN_OUTPUT = []
PALIGN_OUTPUT = []
SALIGN_OUTPUT = []

TASK_LIST = config["task"]
print(TASK_LIST)

if "build_bidic" in TASK_LIST:
    BUILD_OUTPUT.append(DIC)

if "build_hunalign_dic" in TASK_LIST:
    BUILD_OUTPUT.append(HUNALIGN_DIC)

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


print(BUILD_OUTPUT)
print(OUTPUT)
print(PPROC_OUTPUT)
print(DALIGN_OUTPUT)
print(PALIGN_OUTPUT)
print(SALIGN_OUTPUT)


rule all:
    input:
        expand("{target}", target=BUILD_OUTPUT),
        expand("{target}", target=OUTPUT),
        expand("{target}", target=PPROC_OUTPUT),
        expand("{target}", target=DALIGN_OUTPUT),
        expand("{target}", target=PALIGN_OUTPUT),
        expand("{target}", target=SALIGN_OUTPUT)

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
    input:
        '{dir}/{lang}/plain_text.xz'
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
        '  url1=$(echo "$line" | cut -f 1);'
        '  url2=$(echo "$line" | cut -f 2);'
        '  n1=$(xzcat -T 0 {input[1]} | grep -n "^$url1$" | head -n 1 | cut -d: -f1);'
        '  n2=$(xzcat -T 0 {input[2]} | grep -n "^$url2$" | head -n 1 | cut -d: -f1);'
        '  t1=$(xzcat -T 0 {input[3]} | sed -n "${{n1}}p");'
        '  t2=$(xzcat -T 0 {input[4]} | sed -n "${{n2}}p");'
        '  echo -e "k\t{LANG1}\t$url1\t$t1\t{LANG2}\t$url2\t$t2";'
        'done < <(xzcat -T 0 {input[0]}) | gzip -c > $html_gz;'
        '{PROFILING} strand-align -i $html_gz -o $strand_out -ib64 -ah;'
#        '{PROFILING} strand-align -i $html_gz -o $strand_out -ib64 -ob64 -ah;'
        'cat ${{strand_out}}.{LANG2}-{LANG1}.ann | awk -F$\'\t\' \'{{print $2"\\t"$1"\\t"$3"\\t"$4"\\t"$5"\\t"$7"\\t"$6}}\' > {output[0]};'
        'cat ${{strand_out}}.{LANG2}-{LANG1} | awk -F$\'\t\' \'{{print $3"\\t"$4"\\t"$1"\\t"$2"\\t"$5}}\' > {output[1]};'
        'rm $html_gz;'
        'rm $strand_out;'

rule stranded_hunalign:
    """ HUNALIGN with STRAND as input
    """
    input:
        ann = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}.ann',
        bitext = f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}{PALIGN_SUFFIX}',
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.srd.hun.xz'
    shell:
        '{PROFILING} ./scripts/pcm-hunalign-strand.py {input.ann} {input.bitext} -ha {HUNALIGN}/src/hunalign -dic {HUNALIGN_DIC} -s1 "{SENTTOK1}" -s2 "{SENTTOK2}" -w1 "{WORDTOK1}" -w2 "{WORDTOK2}" -t {TMP_DIR} | xz -T 0 > {output}'

# ================================== HUNALIGN SEGMENT ALIGNMENT ================================== #

"""
rule hunalign:
    input:
        dic = f'{HUNALIGN_DIC}',
        docalign = f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.xz",
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.hun.xz'
    shell:
        'xzcat -T 0 {input.docalign} | {PROFILING} ./scripts/pcm-hunalign-bidoc.py -d {input.dic} -t {TMP_DIR} --lang1 {LANG1} --lang2 {LANG2} --hunalign-dir {HUNALIGN}/src/hunalign --sent-tokeniser_sl "{SENTTOK1}" --sent-tokeniser_tl "{SENTTOK2}" | xz -T 0 > {output};'
"""

rule hunalign_old:
    input:
        dic = f'{HUNALIGN_DIC}',
        docalign = f"{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.xz",
        url1 = f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/url.xz",
        url2 = f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/url.xz",
        tok1 = f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG1}/plain_tokenized.xz",
        tok2 = f"{DATA_DIR}/preprocess/{{target}}/{PPROC}/bitextorlang/{LANG2}/plain_tokenized.xz"
    output:
        f'{TRANSIENT_DIR}/{{target}}/bitext{DALIGN_SUFFIX}.hun.xz'
    shell:
        'tokenized=$(mktemp "{TMP_DIR}/tokenized.docalign.XXXXXX");'
        'while IFS= read -r line;'
        'do url1=$(echo "$line" | cut -f 1);'
        'url2=$(echo "$line" | cut -f 2);'
        # TODO: fix bug when same url appears twice
        # TODO: escape '$'
        'n1=$(xzcat -T 0 {input.url1} | grep -n "^$url1$" | head -n 1 | cut -d: -f1);'
        'n2=$(xzcat -T 0 {input.url2} | grep -n "^$url2$" | head -n 1 | cut -d: -f1);'
        't1=$(xzcat -T 0 {input.tok1} | sed -n "${{n1}}p");'
        't2=$(xzcat -T 0 {input.tok2} | sed -n "${{n2}}p");'
        'echo -e "$t1\t$t2";'
        'done < <(xzcat -T 0 {input[1]}) > $tokenized;'
        'paste <(xzcat -T 0 {input.docalign}) $tokenized | {PROFILING} ./scripts/pcm-hunalign-bidoc-old.py -d {input.dic} -t {TMP_DIR} --lang1 {LANG1} --lang2 {LANG2} --hunalign-dir "{HUNALIGN}/src/hunalign" --sent-tokeniser_sl "{SENTTOK1}" --sent-tokeniser_tl "{SENTTOK2}" | xz -T 0 > {output};'

        'rm $tokenized;'

rule hunalign_dic:
    """ Build hunalign dictionary
    """
    input:
        expand("{dic}", dic=DIC)
    output:
        f'{HUNALIGN_DIC}'
    run:
        with open(output[0], "wt") as outw:
            with open(input[0], "rt") as inr:
                header = inr.readline().strip()
                langs = header.split("\t")
                if langs[0] == LANG1 and langs[1] == LANG2:
                    inverse = True
                else:
                    inverse = False
                for inline in inr:
                    columns = inline.strip().split("\t")
                    if inverse:
                        outw.write(columns[1]+" @ "+columns[0]+"\n")
                    else:
                        outw.write(columns[0]+" @ "+columns[1]+"\n")

# ================================= TRAIN BILINGUAL DICTIONARIES ================================= #

rule tokenize_file_l1:
    input:
        expand("{dataset}.{lang}.xz", dataset=TRAIN_PREFIXES, lang=LANG1)
    output:
        f"{{dir}}/corpus.tok.{LANG1}.xz"
    shell:
        "mkdir -p {wildcards.dir}; "
        "xzcat -T 0 -f {input} | sed \"s/&apos;/'/g\" | sed 's/&quot;/\"/g' | sed 's/&amp;/\&/g' | {WORDTOK1} | xz -T 0 > {output}"

rule tokenize_file_l2:
    input:
        expand("{dataset}.{lang}.xz", dataset=TRAIN_PREFIXES, lang=LANG2)
    output:
        f"{{dir}}/corpus.tok.{LANG2}.xz"
    shell:
        "mkdir -p {wildcards.dir}; "
        "xzcat -T 0 -f {input} | sed \"s/&apos;/'/g\" | sed 's/&quot;/\"/g' | sed 's/&amp;/\&/g' | {WORDTOK2} | xz -T 0 > {output}"

rule lowercase:
    input:
        "{prefix}.tok.{lang}.xz"
    output:
        "{prefix}.tok.low.{lang}"
    shell:
        "xzcat {input} | {PROFILING} {MOSES}/scripts/tokenizer/lowercase.perl > {output}"

rule clean:
    input:
        f"{{prefix}}.tok.low.{LANG1}",
        f"{{prefix}}.tok.low.{LANG2}"
    output:
        f"{{prefix}}.clean.{LANG1}",
        f"{{prefix}}.clean.{LANG2}"
    shell:
        "{PROFILING} perl ./utils/clean-corpus-n.perl {wildcards.prefix}.tok.low {LANG1} {LANG2} {wildcards.prefix}.clean 1 80 {wildcards.prefix}.lines-retained"

rule plain2snt:
    input:
        l1 = f"{PPROC_CORPUS_DIR}/corpus.clean.{LANG1}",
        l2 = f"{PPROC_CORPUS_DIR}/corpus.clean.{LANG2}"
    output:
        snt_2_1 = f"{MGIZA_MODEL_DIR}/corpus.{LANG2}-{LANG1}-int-train.snt",
        snt_1_2 = f"{MGIZA_MODEL_DIR}/corpus.{LANG1}-{LANG2}-int-train.snt",
        vcb1 = f"{MGIZA_MODEL_DIR}/corpus.{LANG1}.vcb",
        vcb2 = f"{MGIZA_MODEL_DIR}/corpus.{LANG2}.vcb"
    priority:
        40
    shell:
        "mkdir -p {MGIZA_MODEL_DIR}; "
        "{MGIZA}/mgizapp/bin/plain2snt {input.l1} {input.l2} 2> /dev/null > /dev/null; "
        "mv {PPROC_CORPUS_DIR}/corpus.clean.{LANG1}_corpus.clean.{LANG2}.snt {output.snt_2_1}; "
        "mv {PPROC_CORPUS_DIR}/corpus.clean.{LANG2}_corpus.clean.{LANG1}.snt {output.snt_1_2}; "
        "cp {PPROC_CORPUS_DIR}/corpus.clean.{LANG1}.vcb {output.vcb1}; "
        "cp {PPROC_CORPUS_DIR}/corpus.clean.{LANG2}.vcb {output.vcb2}; "

rule mkcls:
    input:
        f"{PPROC_CORPUS_DIR}/corpus.clean.{{lang}}"
    output:
        f"{MGIZA_MODEL_DIR}/corpus.{{lang}}.vcb.classes"
    priority:
        40
    shell:
        "{PROFILING} {MKCLS} -c50 -n2 -p{input} -V{output} opt 2> /dev/null > /dev/null"

rule snt2cooc:
    input:
        vcb1 = "{prefix}.{l1}.vcb",
        vcb2 = "{prefix}.{l2}.vcb",
        vcb1cls = "{prefix}.{l1}.vcb.classes",
        vcb2cls = "{prefix}.{l2}.vcb.classes",
        snt = "{prefix}.{l2}-{l1}-int-train.snt"
    output:
        "{prefix}.{l2}-{l1}.cooc"
    shell:
        "{PROFILING} {MGIZA}/mgizapp/bin/snt2cooc {output} {input.vcb1} {input.vcb2} {input.snt} 2> /dev/null"

rule mgiza:
    input:
        vcb1 = "{prefix}.{l1}.vcb",
        vcb2 = "{prefix}.{l2}.vcb",
        snt = "{prefix}.{l2}-{l1}-int-train.snt",
        cooc = "{prefix}.{l2}-{l1}.cooc"
    output:
        "{prefix}.{l2}-{l1}.t3.final"
    shell:
        "{PROFILING} {MGIZA}/mgizapp/bin/mgiza -ncpus 8 -CoocurrenceFile {input.cooc} -c {input.snt} -m1 5 -m2 0 -m3 3 -m4 3 -mh 5 -m5 0 -model1dumpfrequency 1 -o {wildcards.prefix}.{wildcards.l2}-{wildcards.l1} -s {input.vcb1} -t {input.vcb2} -emprobforempty 0.0 -probsmooth 1e-7 2> /dev/null > /dev/null"

rule filter_dics:
    input:
        "{prefix}.vcb"
    output:
        "{prefix}.filtered.vcb"
    shell:
        "cat {input} | egrep ' [^ ][^ ]+$' > {output}"

rule symmetrise_dic:
    """ Obtaining the harmonic probability of each pair of words in both directions and filtering out those with less than p=0.2; printing the dictionary
    """
    input:
        vcb1 = f"{MGIZA_MODEL_DIR}/corpus.{LANG1}.filtered.vcb",
        vcb2 = f"{MGIZA_MODEL_DIR}/corpus.{LANG2}.filtered.vcb",
        t3_1 = f"{MGIZA_MODEL_DIR}/corpus.{LANG1}-{LANG2}.t3.final",
        t3_2 = f"{MGIZA_MODEL_DIR}/corpus.{LANG2}-{LANG1}.t3.final"
    output:
        f"{DIC}"
    run:
        svocabulary = {}
        tvocabulary = {}
        svcb = open(input.vcb1, "r")
        tvcb = open(input.vcb2, "r")
        for line in svcb:
            item = line.strip().split(" ")
            svocabulary[item[0]] = item[1]

        for line in tvcb:
            item = line.strip().split(" ")
            tvocabulary[item[0]] = item[1]

        t3dic = {}
        t3s = open(input.t3_1, "r")
        t3t = open(input.t3_2, "r")
        for line in t3t:
            item = line.strip().split(" ")
            if item[1] in t3dic:
                t3dic[item[1]][item[0]] = item[2]
            else:
                t3dic[item[1]] = {}
                t3dic[item[1]][item[0]] = item[2]

        dic = open(output[0], "wt")
        dic.write(LANG1+"\t"+LANG2+"\n")
        for line in t3s:
            item = line.strip().split(" ")
            if item[0] in t3dic:
                if item[1] in t3dic[item[0]]:
                    value1 = float(t3dic[item[0]][item[1]])
                    value2 = float(item[2])
                    hmean = 2/((1/value1)+(1/value2))

                    if hmean > 0.1:
                        if item[1] in svocabulary and item[0] in tvocabulary:
                            word1 = svocabulary[item[1]]
                            word2 = tvocabulary[item[0]]
                            if word1.isalpha() or word2.isalpha():
                                dic.write("{0}\t{1}\n".format(word1, word2))
        svcb.close()
        tvcb.close()
        t3s.close()
        t3t.close()
        dic.close()
        os.sync()
