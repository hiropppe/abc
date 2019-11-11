import subprocess
import tldextract

from pathlib import Path


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

if "boilerpipeCleaning" in config and config["boilerpipeCleaning"]==True:
  BOILERPIPE_CLEANING = '--boilerpipe'
else:
  BOILERPIPE_CLEANING = ''

if "pdf-converter" in config and config["pdf-converter"]=="pdf-extract":
  USE_PDF_EXTRACT = "--pdfextract"
else:
  USE_PDF_EXTRACT = ""

if "plainTextHashes" in config:
  PLAINTEXTHASHES= "--input_hash "+config["plainTextHashes"]
else:
  PLAINTEXTHASHES=""

PARSER = config.get("parser", "")


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


def system_check(cmd):
    subprocess.check_call(cmd, shell=True)


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


domainkey2hosts = create_domainkey2hosts(hosts)

# If file domains.gz exists in the permanent directory, the dictionary domainKey2Hosts is filtered to contain only those TLD in this file
domainkey2hosts = filter_tld(domainkey2hosts)
print(domainkey2hosts)

#================================== START SNAKEMAKE================================#
OUTPUT = []
PPROCOUTPUT=[]

if config.get("onlyConcat", False):
    for tld in domainkey2hosts.keys():
        OUTPUT.append(f"{DATA_DIR:}/preprocess/{tld:s}/concat.warc.gz")

elif config.get("onlyPreprocessing", False):
    for tld in domainkey2hosts.keys():
        for lang in PPROCLANGS:
            for file in FILES:
                PPROCOUTPUT.append(f"{DATA_DIR:}/preprocess/{tld:s}/{PPROC:s}/bitextorlang/{lang:s}/{file:s}")

print(OUTPUT)
print(PPROCOUTPUT)

rule all:
    input:
        expand("{target}", target=OUTPUT),
        expand("{preprocess}", preprocess=PPROCOUTPUT)


rule concat_subdomains:
    input:
        get_domain_hosts
    output:
        f"{DATA_DIR:}/preprocess/{{target}}/concat.warc.gz"
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
        f'{DATA_DIR:}/preprocess/{{domain}}/concat.warc.gz'
    output:
        hash=f'{DATA_DIR:}/preprocess/{{domain}}/w2p/bitextorlang/plain_text_hashes.xz',
        files=expand('{data}/preprocess/{{domain}}/w2p/bitextorlang/{lang}/{file}', data=DATA_DIR, lang=PPROCLANGS, file=FILES)
    params:
        folder='{data}/preprocess/{{domain}}/w2p/bitextorlang'.format(data=DATA_DIR)
    priority: 8
    threads: 2
    shell:
        'mkdir -p {params.folder};'
        '{PROFILING} ./scripts/warc2htmlwarc.py {CLEANHTML} {FTFY} --input {input} {USE_PDF_EXTRACT} | {PROFILING} nice ionice -c 3 ./scripts/warc2preprocess.py --input - {PPROCLANGSOPT} --lang1 {LANG1} --lang2 {LANG2} {BOILERPIPE_CLEANING} --langid {LANGID} --output-dir {params.folder} --output_hash {output.hash} {PLAINTEXTHASHES} {PARSER}; '
        'for lang in {PPROCLANGS}; do '
        '  if [ ! -f {params.folder}/$lang/plain_text.xz ]; then >&2 '
        '    echo "WARNING: no \'$lang\' data found in {wildcards.domain}. Creating empty files instead";'
        '    mkdir -p {params.folder}/$lang;'
        '    touch {params.folder}/$lang/plain_text {params.folder}/$lang/mime {params.folder}/$lang/url {params.folder}/$lang/normalized_html {params.folder}/$lang/deboilerplate_html ;'
        '    xz {params.folder}/$lang/*;'
        '  fi ; '
        'done'
