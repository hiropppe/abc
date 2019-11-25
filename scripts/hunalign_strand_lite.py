#!/usr/bin/env python3

import numpy as np
import click
import os
import base64
import subprocess

from tempfile import NamedTemporaryFile
from tqdm import tqdm
from external_processor import ExternalTextProcessor


def align(doc,
          url1, url2,
          src_len, tgt_len,
          src_senttok, tgt_senttok,
          src_morph, tgt_morph,
          src_wordtok, tgt_wordtok,
          hundir, hundic,
          tmp_dir,
          cost_threshould,
          loosy,
          batch_size):

    data = []
    for line in doc:
        line = line.split("\t")
        src_seq = int(line[0])
        src_text = line[1]
        tgt_seq = int(line[2])
        tgt_text = line[3]
        cost = float(line[4])
        data.append((src_seq, src_text, tgt_seq, tgt_text, cost))

    for i, each_data in enumerate(data):
        cost = float(each_data[4])

        src_text = each_data[1]
        tgt_text = each_data[3]

        src_sents = sent_tokenize(src_text, src_senttok)
        tgt_sents = sent_tokenize(tgt_text, tgt_senttok)

        if cost < cost_threshould:
            if len(src_sents) == len(tgt_sents) == 1:
                print(f"{url1}\t{url2}\t{src_sents[0]}\t{tgt_sents[0]}\t1.0")
            elif len(src_sents) + len(tgt_sents) > 2:
                align_paragraph(src_sents, tgt_sents, src_wordtok,
                                tgt_wordtok, url1, url2, hundir, hundic, tmp_dir)


def align_paragraph(src_sents, tgt_sents, src_wordtok, tgt_wordtok, url1, url2, hundir, hundic, tmp_dir):
    src_words = word_tokenize(src_sents, src_wordtok)
    tgt_words = word_tokenize(tgt_sents, tgt_wordtok)

    assert len(src_sents) == len(src_words)
    assert len(tgt_sents) == len(tgt_words)

    try:
        tmp1 = NamedTemporaryFile(delete=False, dir=tmp_dir)
        tmp2 = NamedTemporaryFile(delete=False, dir=tmp_dir)
        tmp1_tok = NamedTemporaryFile(delete=False, dir=tmp_dir)
        tmp2_tok = NamedTemporaryFile(delete=False, dir=tmp_dir)

        for i in range(len(src_sents)):
            tmp1.write(src_sents[i].encode() + b"\n")
            tmp1_tok.write(src_words[i].lower().encode() + b"\n")

        for j in range(len(tgt_sents)):
            tmp2.write(tgt_sents[j].encode() + b"\n")
            tmp2_tok.write(tgt_words[j].lower().encode() + b"\n")

        tmp1.close()
        tmp1_tok.close()
        tmp2.close()
        tmp2_tok.close()

        hunalign(tmp1_tok.name, tmp2_tok.name, tmp1.name,
                 tmp2.name, url1, url2, hundir, hundic)

    finally:
        try:
            os.remove(tmp1.name)
            os.remove(tmp1_tok.name)
            os.remove(tmp2.name)
            os.remove(tmp2_tok.name)
        except:  # noqa
            pass


def generate_batch(seq, n_gaps):
    gaps = np.array([abs(seq[min(i+1, len(seq)-1)] - seq[i]) for i in range(len(seq))])
    gaps_i = np.argsort(-gaps) + 1
    gaps_i = gaps_i[:min(n_gaps, len(gaps_i))]
    offset = 0
    for i in sorted(gaps_i):
        yield(seq[offset: i])
        offset = i


def batch_align(batch, url1, url2, src_senttok, tgt_senttok, src_wordtok, tgt_wordtok, hundir, hundic, tmp_dir):
    try:
        tmp1 = NamedTemporaryFile(delete=False, dir=tmp_dir)
        tmp2 = NamedTemporaryFile(delete=False, dir=tmp_dir)
        tmp1_tok = NamedTemporaryFile(delete=False, dir=tmp_dir)
        tmp2_tok = NamedTemporaryFile(delete=False, dir=tmp_dir)

        for each_data in batch:
            src_text = each_data[1]
            tgt_text = each_data[3]

            src_sents = sent_tokenize(src_text, src_senttok)
            tgt_sents = sent_tokenize(tgt_text, tgt_senttok)
            src_words = word_tokenize(src_sents, src_wordtok)
            tgt_words = word_tokenize(tgt_sents, tgt_wordtok)

            for i in range(len(src_sents)):
                tmp1.write(src_sents[i].encode() + b"\n")
                tmp1_tok.write(src_words[i].lower().encode() + b"\n")

            for j in range(len(tgt_sents)):
                tmp2.write(tgt_sents[j].encode() + b"\n")
                tmp2_tok.write(tgt_words[j].lower().encode() + b"\n")

        tmp1.close()
        tmp1_tok.close()
        tmp2.close()
        tmp2_tok.close()

        hunalign(tmp1_tok.name, tmp2_tok.name, tmp1.name, tmp2.name,
                 url1, url2, hundir, hundic, quality_threshould=-1)
    finally:
        try:
            os.remove(tmp1.name)
            os.remove(tmp1_tok.name)
            os.remove(tmp2.name)
            os.remove(tmp2_tok.name)
        except:  # noqa
            pass


def hunalign(file1, file2, file1orig, file2orig, filename1, filename2, hundir, hundic, quality_threshould=0.1):
    filereader1 = open(file1orig, "r")
    filereader2 = open(file2orig, "r")

    hunalign_output = run_hunaligner(file1, file2, hundic, hundir)
    try:
        prev_hun = next(hunalign_output).strip()
        if prev_hun.startswith(b"Quality "):
            quality = float(prev_hun.split()[1])
        else:
            quality = 0.0

        prev_hun = next(hunalign_output).strip()
        prev_fields = prev_hun.split(b"\t")
        if int(prev_fields[0]) > 0:
            for i in range(int(prev_fields[0])):
                line1 = filereader1.readline().strip()

        if int(prev_fields[1]) > 0:
            for i in range(int(prev_fields[1])):
                line2 = filereader2.readline().strip()
    except StopIteration:
        prev_hun = ""

    if quality < quality_threshould:
        return False

    for line_h in hunalign_output:
        hun_line = line_h.strip()
        last_position1 = filereader1.tell()
        last_position2 = filereader2.tell()
        line1 = filereader1.readline().strip()
        line2 = filereader2.readline().strip()
        prev_fields = prev_hun.split(b"\t")
        hunalign_fields = hun_line.split(b"\t")

        if float(prev_fields[2]) == -0.3:
            if int(hunalign_fields[0]) == int(prev_fields[0]):
                line1 = ""
                filereader1.seek(last_position1)
            elif int(hunalign_fields[1]) == int(prev_fields[1]):
                line2 = ""
                filereader2.seek(last_position2)

        if int(hunalign_fields[0]) - int(prev_fields[0]) > 1:
            for i in range((int(hunalign_fields[0]) - int(prev_fields[0])) - 1):
                line1 += " " + filereader1.readline().strip()

        if int(hunalign_fields[1]) - int(prev_fields[1]) > 1:
            for i in range((int(hunalign_fields[1]) - int(prev_fields[1])) - 1):
                line2 += " " + filereader2.readline().strip()

        print("{0}\t{1}\t{2}\t{3}\t{4}".format(filename1, filename2,
                                               line1, line2, prev_fields[2].decode("utf8")))

        prev_hun = hun_line

    filereader1.close()
    filereader2.close()

    return True


def sent_tokenize(text, senttok):
    proc_sent = ExternalTextProcessor(senttok.split(' '))
    # content = base64.b64decode(text).decode("utf-8").replace("\t", " ")
    content = text.replace("\t", " ")
    sents = proc_sent.process(content).strip()
    sents = [s.strip() for s in sents.split("\n") if s.strip()]
    return sents


def word_tokenize(sents, wordtok):
    proc_word = ExternalTextProcessor(wordtok.split(' '))
    ret = []
    for sent in sents:
        words = proc_word.process(sent)
        ret.append(words.strip())
    return ret


def run_hunaligner(filename_s, filename_t, dic, hunaligndir):
    # option -ppthresh=10?
    if dic is None or dic == "":
        if hunaligndir is None:
            hunalign = [os.path.dirname(os.path.abspath(__file__)) + "hunalign", "-realign", "/dev/null", filename_s,
                        filename_t]
        else:
            hunalign = [hunaligndir + "/hunalign", "-realign", "/dev/null", filename_s, filename_t]
    else:
        if hunaligndir is None:
            hunalign = [os.path.dirname(os.path.abspath(__file__)) +
                        "hunalign", dic, filename_s, filename_t]
        else:
            hunalign = [hunaligndir + "/hunalign", dic, filename_s, filename_t]

    p = subprocess.Popen(hunalign, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line_p in p.stderr:
        if line_p.startswith(b"Quality"):
            yield line_p
    for line_o in p.stdout:
        yield line_o


@click.command()
@click.argument("align_ann")
@click.argument("align_data")
@click.option("--hunalign_dir", "-ha", help="")
@click.option("--hunalign_dic", "-dic", help="")
@click.option("--sent_tokenizer1", "-s1", help="")
@click.option("--sent_tokenizer2", "-s2", help="")
@click.option("--morph_analyzer1", "-m1", help="")
@click.option("--morph_analyzer2", "-m2", help="")
@click.option("--word_tokenizer1", "-w1", help="")
@click.option("--word_tokenizer2", "-w2", help="")
@click.option("--tmp_dir", "-t", help="")
@click.option("--dp_threshould", default=0.5, help="")
@click.option("--cost_threshould", default=0.2, help="")
@click.option("--loosy", is_flag=True, default=True, help="")
@click.option("--batch_size", default=10, help="")
def main(align_ann,
         align_data,
         hunalign_dir,
         hunalign_dic,
         sent_tokenizer1,
         sent_tokenizer2,
         morph_analyzer1,
         morph_analyzer2,
         word_tokenizer1,
         word_tokenizer2,
         tmp_dir,
         dp_threshould,
         cost_threshould,
         loosy,
         batch_size):

    with open(align_data) as datain:
        all_doc = [line.strip() for line in datain.readlines()]

    with open(align_ann) as annin:
        for ann in tqdm([ann for ann in annin]):
            ann = ann.strip().split("\t")
            url1 = ann[0]
            url2 = ann[1]
            offset = int(ann[2])
            n_lines = int(ann[3])
            dp = float(ann[4])
            src_len = int(ann[5])
            tgt_len = int(ann[6])

            if dp > dp_threshould:
                continue

            doc = all_doc[offset:offset+n_lines]

            align(doc,
                  url1, url2,
                  src_len, tgt_len,
                  sent_tokenizer1, sent_tokenizer2,
                  morph_analyzer1, morph_analyzer2,
                  word_tokenizer1, word_tokenizer2,
                  hunalign_dir, hunalign_dic,
                  tmp_dir,
                  cost_threshould,
                  loosy, batch_size)


if __name__ == "__main__":
    main()
