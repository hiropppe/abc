import click
import lzma
import numpy as np
import fasttext
import re
import sys

from tqdm import tqdm

from external_processor import ExternalTextProcessor

re_ascii_text = re.compile(r"['!\"#$%&()*+,-./:;<=>?@\[\\\]^_`{|}~\t\s0-9]+")
re_ascii_char = re.compile(r"['!\"#$%&()*+,-./:;<=>?@\[\\\]^_`{|}~0-9]")


def is_ascii_artistic(sent, tok, std_threshould=6.0):
    if re.fullmatch(re_ascii_text, sent):
        return True, 1.0
    else:
        tokens = tok.process(sent).strip().split()
        more_tokens = []
        if "mecab" in tok.cmd[0]:
            for token in tokens:
                if re_ascii_text.fullmatch(token):
                    more_tokens.extend(["-" for c in token])
                else:
                    more_tokens.append(token.lower())
        else:
            more_tokens = ["-" if re_ascii_char.match(token) else token for token in tokens]
        bow = np.zeros(len(set(more_tokens)))
        token2id = {}
        for token in more_tokens:
            if token not in token2id:
                token2id[token] = len(token2id)
            i = token2id[token]
            bow[i] += 1
        std = np.std(bow)
        if std_threshould < std:
            return True, std
        else:
            return False, std


@click.command()
@click.option("--inp", help="input")
@click.option("--lid", help="Language identification implementation")
@click.option("--lid_model", help="fastText LID model path")
@click.option("--lang1", required=True, help="")
@click.option("--lang2", required=True, help="")
@click.option("--tokenizer1", "-t1", required=True, help="")
@click.option("--tokenizer2", "-t2", required=True, help="")
@click.option("--err_out")
def main(inp, lid, lid_model, lang1, lang2, tokenizer1, tokenizer2, err_out):
    try:
        if inp:
            if inp[-2:] == "xz":
                reader = lzma.open(inp)
            else:
                reader = open(inp)
        else:
            reader = sys.stdin

        if err_out:
            err = lzma.open(err_out, "wt")
        else:
            err = sys.stderr

        if lid == "fastText":
            model = fasttext.load_model(lid_model)
        else:
            raise ValueError("LID out of bounds")

        tok1 = ExternalTextProcessor(tokenizer1.split())
        tok2 = ExternalTextProcessor(tokenizer2.split())
        for line in tqdm(reader):
            if isinstance(line, bytes):
                line = line.decode("utf8")
            line = line.strip()
            s1, s2 = line.split("\t")[2:4]

            if s1.strip() == s2.strip():
                print(f"eq: {line}", file=err)
                continue

            ascii1, std1 = is_ascii_artistic(s1, tok1)
            ascii2, std2 = is_ascii_artistic(s2, tok2)
            if ascii1 or ascii2:
                print(f"ascii: {std1:f} {std2:f} {line}", file=err)
                continue

            pred1 = model.predict([s1])[0]
            pred2 = model.predict([s2])[0]
            if pred1[0][0][9:] == lang1 and pred2[0][0][9:] == lang2:
                print(line, file=sys.stdout)
            else:
                print(f"lid: {line}", file=err)

    finally:
        if inp and reader:
            reader.close()
        if err_out and err:
            err.close()


if __name__ == "__main__":
    main()
