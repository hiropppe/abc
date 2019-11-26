#!/usr/bin/env python3

import os
import re
import sys
import argparse
import tldextract

from tqdm import tqdm
from urllib.parse import urlparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../utils")
from common import open_xz_or_gzip_or_plain

oparser = argparse.ArgumentParser(description="usage: %prog [options]\nTool that processes a .ridx (reverse index) "
                                              "file (either from a file or from the standard input) and produces a "
                                              "list of aligned documents. If two ridx files are provided, "
                                              "a bidirectional alignment is performed between them.")
oparser.add_argument('--lang1', dest='lang1', help='')
oparser.add_argument('--lang2', dest='lang2', help='')
oparser.add_argument('--text1', dest='text1', help='File produced by bitextor-warc2preprocess containing the text of '
                     'all the records in the WARC file encoded as base 64 (each line '
                     'corresponds to a record)', required=True)
oparser.add_argument('--text2', dest='text2', help='File produced by bitextor-warc2preprocess containing the text of '
                     'all the records in the WARC file encoded as base 64 (each line '
                     'corresponds to a record)', required=True)
oparser.add_argument('--url1', dest='url1', help='File produced by bitextor-warc2preprocess containing the url of each '
                     'of the records in the WARC file encoded as base 64 (each line '
                     'corresponds to a record)', required=True)
oparser.add_argument('--url2', dest='url2', help='File produced by bitextor-warc2preprocess containing the url of each '
                     'of the records in the WARC file encoded as base 64 (each line '
                     'corresponds to a record)', required=True)
oparser.add_argument("-n", "--num_candidates", help="Amount of alignment candidates taken into account for every file "
                                                    "when performing bidirectional document alignment. This parameter "
                                                    "is set by default to 1, which means that only documents being "
                                                    "mutualy the best alignment option will be aligned. Note that "
                                                    "this option is only used when two ridx files are provided",
                     type=int, dest="candidate_num", default=1)

options = oparser.parse_args()


def more_codes(lang):
    if lang == "ja":
        return ("ja", "jp", "jpn", "japanese", "japan", "j")
    if lang == "en":
        return ("en", "us", "eng", "english", "usa", "e")
    return (lang,)


def read(path):
    ret = []
    with open_xz_or_gzip_or_plain(path) as reader:
        for line in reader:
            line = line.strip()
            ret.append(line)
    return ret


l1_urls = read(options.url1)
l2_urls = read(options.url2)
l1_texts = read(options.text1)
l2_texts = read(options.text2)


re_l1_lang = re.compile(r"(?<=[^a-z])({:s})(?=[^a-z])".format("|".join(more_codes(options.lang1))), flags=re.IGNORECASE)
re_l2_lang = re.compile(r"(?<=[^a-z])({:s})(?=[^a-z])".format("|".join(more_codes(options.lang2))), flags=re.IGNORECASE)
re_sla2 = re.compile("/{2,}")
re_del = re.compile("[-_~]")


def match_url(l1_url, l2_url):
    l1_parsed = urlparse(l1_url)
    l1_netloc = l1_parsed.netloc
    l1_path_query = l1_parsed.path + l1_parsed.query + "#"

    l2_parsed = urlparse(l2_url)
    l2_netloc = l2_parsed.netloc
    l2_path_query = l2_parsed.path + l2_parsed.query + "#"

    if (l1_netloc != l2_netloc and l1_path_query == l2_path_query) \
            or re_del.sub("", re_sla2.sub("/", re_l1_lang.sub("", l1_path_query))) == re_del.sub("", re_sla2.sub("/", re_l2_lang.sub("", l2_path_query))):
        return True
    else:
        return False


matches = []
for i in tqdm(range(len(l1_urls))):
    url1 = l1_urls[i]
    for j, url2 in enumerate(l2_urls):
        if match_url(url1, url2):
            # The index starts at 1 for use with the -n option of sed
            matches.append((i+1, j+1))
            break


for m in matches:
    i, j = m
    print("{0}\t{1}\t{2}\t{3}".format(i, j, l1_texts[i-1], l2_texts[j-1]))
