#!/usr/bin/env python3

import base64
import click
import fasttext
import gzip
import lzma
import sys
import re

from external_processor import ExternalTextProcessor
from strand import parsers

re_tag = re.compile(r"^\[(START|END):([^\]]+)\]$")
re_space = re.compile(r"[\s\u3000]+")


def write_sentences(html, lang, sent_tokenizer, outfile, lid=None):
    html = base64.b64decode(html).decode("utf8")
    tagchunks = parsers.parse(html, lang).split("\n")
    chunks = [re_space.sub(" ", tc).strip() for tc in tagchunks if not re_tag.match(tc.strip())]
    proc_sent = ExternalTextProcessor(sent_tokenizer.split(' '))
    dedup = set()
    n_sents = 0
    for chunk in chunks:
        if chunk.strip():
            if lid:
                pred = lid.predict([chunk])[0]
                if pred[0][0][9:] != lang:
                    continue
            tokenized_segs = proc_sent.process(chunk).strip()
            for sent in tokenized_segs.split("\n"):
                if sent not in dedup:
                    print(sent, file=outfile)
                    dedup.add(sent)
                    n_sents += 1
    return n_sents


@click.command()
@click.option("--input", "-i", help="File containing the set of aliged documents")
@click.option("--prefix", "-p", help="Output prefix")
@click.option("--slang", "-sl", required=True, help="Source language code")
@click.option("--tlang", "-tl", required=True, help="Source language code")
@click.option("--sent_tokenizer1", "-s1", required=True, help="Sentence tokenizer for source language")
@click.option("--sent_tokenizer2", "-s2", required=True, help="Sentence tokenizer for target language")
@click.option("--lid", help="Language identification implementation")
@click.option("--lid_model", help="fastText LID model path")
def main(input, prefix, slang, tlang, sent_tokenizer1, sent_tokenizer2, lid, lid_model):
    if input:
        if input.endswith(".xz"):
            reader = lzma.open(input)
        if input.endswith(".gz"):
            reader = gzip.open(input)
        else:
            reader = open(input)
    else:
        reader = sys.stdin

    if lid:
        if lid == "fastText":
            model = fasttext.load_model(lid_model)
        else:
            raise ValueError("LID out of bounds")
    else:
        model = None

    s_cc = prefix + ".cc." + slang
    t_cc = prefix + ".cc." + tlang
    cc_offset = prefix + ".cc.offset"

    with open(s_cc, "w") as sw, open(t_cc, "w") as tw, open(cc_offset, "w") as ow:
        s_offset = 0
        t_offset = 0
        for line in reader:
            if isinstance(line, bytes):
                line = line.decode("utf8")
            fields = line.split("\t")
            n1 = fields[0]
            n2 = fields[1]
            html1 = fields[2]
            html2 = fields[3]

            s_len = write_sentences(html1, slang, sent_tokenizer1, sw, model)
            t_len = write_sentences(html2, tlang, sent_tokenizer2, tw, model)

            print(f"{n1}\t{s_offset}\t{s_len}\t{n2}\t{t_offset}\t{t_len}", file=ow)

            s_offset += s_len
            t_offset += t_len


if __name__ == "__main__":
    main()
