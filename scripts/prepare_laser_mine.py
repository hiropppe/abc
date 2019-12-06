#!/usr/bin/env python3

import click
import lzma
import sys
import base64
import string

from external_processor import ExternalTextProcessor


def write_sentences(b64_text, sent_tokenizer, outfile):
    proc_sent = ExternalTextProcessor(sent_tokenizer.split(' '))
    content = base64.b64decode(b64_text).decode("utf-8").replace("\t", " ")
    tokenized_segs = proc_sent.process(content).strip()
    n_sents = 0
    for sent in tokenized_segs.split("\n"):
        if len(sent) < 1000 and sum([1 for m in sent if m in string.punctuation + string.digits]) < len(sent) // 2:
            print(sent, file=outfile)
            n_sents += 1
    return n_sents


@click.command()
@click.option("--input", "-i", help="File containing the set of aliged documents")
@click.option("--prefix", "-p", help="Output prefix")
@click.option("--slang", "-sl", required=True, help="Source language code")
@click.option("--tlang", "-tl", required=True, help="Source language code")
@click.option("--sent_tokenizer1", "-s1", required=True, help="Sentence tokenizer for source language")
@click.option("--sent_tokenizer2", "-s2", required=True, help="Sentence tokenizer for target language")
def main(input, prefix, slang, tlang, sent_tokenizer1, sent_tokenizer2):
    if input:
        if input.endswith(".xz"):
            reader = lzma.open(input)
        else:
            reader = open(input)
    else:
        reader = sys.stdin

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
            url1 = fields[0]
            url2 = fields[1]
            b64_text1 = fields[2]
            b64_text2 = fields[3]

            s_len = write_sentences(b64_text1, sent_tokenizer1, sw)
            t_len = write_sentences(b64_text2, sent_tokenizer2, tw)

            print(f"{url1}\t{s_offset}\t{s_len}\t{url2}\t{t_offset}\t{t_len}", file=ow)

            s_offset += s_len
            t_offset += t_len


if __name__ == "__main__":
    main()
