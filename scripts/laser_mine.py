#!/usr/bin/env python3

import click
import faiss
import lzma
import numpy as np
import os
import re
import sys
import tempfile
import time
import torch
import torch.nn as nn

from attrdict import AttrDict
from pathlib import Path

# get environment
assert os.environ.get('LASER'), 'Please set the enviornment variable LASER'
LASER = os.environ['LASER']

sys.path.append(LASER + '/source')
sys.path.append(LASER + '/source/lib')
sys.path.append(LASER + '/source/tools')
from embed import SentenceEncoder, EncodeLoad, EncodeFile, EmbedLoad, buffered_read, buffered_arange, convert_padding_direction, Encoder, EncodeTime, EncodeFilep, EmbedMmap  # noqa
from mine_bitexts import TextLoadUnify, knn, knnGPU, knnCPU, score, score_candidates
from text_processing import Token, BPEfastApply  # noqa


###############################################################################
#
# Embed Main
#
###############################################################################

def Embed(tmpdir, ifname, encoder, token_lang, bpe_codes, buffer_size, verbose):
    output = os.path.join(tmpdir, 'emb')

    if token_lang != '--':
        tok_fname = os.path.join(tmpdir, 'tok')
        Token(ifname,
              tok_fname,
              lang=token_lang,
              romanize=True if token_lang == 'el' else False,
              lower_case=True, gzip=False,
              verbose=verbose, over_write=False)
        ifname = tok_fname

    if bpe_codes:
        bpe_fname = os.path.join(tmpdir, 'bpe')
        BPEfastApply(ifname,
                     bpe_fname,
                     bpe_codes,
                     verbose=verbose, over_write=False)
        ifname = bpe_fname

    EncodeFile(encoder,
               ifname,
               output,
               verbose=verbose, over_write=False,
               buffer_size=buffer_size)

    return output


###############################################################################
#
# Mine Main
#
###############################################################################

def Mine(src_doc_ind, trg_doc_ind, src, trg, encoding, src_embeddings, trg_embeddings, output, unify, mode, retrieval, margin, neighborhood, gpu, dim, threshold, verbose):
    print('LASER: tool to search, score or mine bitexts', file=sys.stderr)
    if gpu:
        print(' - knn will run on all available GPUs (recommended)', file=sys.stderr)
    else:
        print(' - knn will run on CPU (slow)', file=sys.stderr)

    args = AttrDict({"encoding": encoding, "unify": unify, "verbose": verbose})
    src_inds, src_sents = TextLoadUnify(src, args)
    trg_inds, trg_sents = TextLoadUnify(trg, args)

    def unique_embeddings(emb, ind, verbose=False):
        aux = {j: i for i, j in enumerate(ind)}
        if verbose:
            print(' - unify embeddings: {:d} -> {:d}'.format(len(emb), len(aux)), file=sys.stderr)
        return emb[[aux[i] for i in range(len(aux))]]

    # load the embeddings
    x = EmbedLoad(src_embeddings, dim, verbose=verbose)
    if unify:
        x = unique_embeddings(x, src_inds, verbose)
    faiss.normalize_L2(x)
    y = EmbedLoad(trg_embeddings, dim, verbose=verbose)
    if unify:
        y = unique_embeddings(y, trg_inds, verbose)
    faiss.normalize_L2(y)

    # calculate knn in both directions
    if retrieval != 'bwd':
        if verbose:
            print(' - perform {:d}-nn source against target'.format(neighborhood), file=sys.stderr)
        x2y_sim, x2y_ind = knn(x, y, min(y.shape[0], neighborhood), gpu)
        x2y_mean = x2y_sim.mean(axis=1)

    if retrieval != 'fwd':
        if verbose:
            print(' - perform {:d}-nn target against source'.format(neighborhood), file=sys.stderr)
        y2x_sim, y2x_ind = knn(y, x, min(x.shape[0], neighborhood), gpu)
        y2x_mean = y2x_sim.mean(axis=1)

    # margin function
    if margin == 'absolute':
        def margin(a, b): return a
    elif margin == 'distance':
        def margin(a, b): return a - b
    else:  # margin == 'ratio':
        def margin(a, b): return a / b

    if output:
        if output.endswith('.xz'):
            fout = lzma.open(output, mode='at', encoding=encoding,  errors='surrogateescape')
        else:
            fout = open(output, mode='a', encoding=encoding, errors='surrogateescape')
    else:
        output = "stdout"
        fout = sys.stdout

    if mode == 'search':
        if verbose:
            print(' - Searching for closest sentences in target', file=sys.stderr)
            print(' - writing alignments to {:s}'.format(output), file=sys.stderr)
        scores = score_candidates(x, y, x2y_ind, x2y_mean, y2x_mean, margin, verbose)
        best = x2y_ind[np.arange(x.shape[0]), scores.argmax(axis=1)]

        nbex = x.shape[0]
        ref = np.linspace(0, nbex-1, nbex).astype(int)  # [0, nbex)
        err = nbex - np.equal(best.reshape(nbex), ref).astype(int).sum()
        print(' - errors: {:d}={:.2f}%'.format(err, 100*err/nbex), file=sys.stderr)
        for i in src_inds:
            print(trg_sents[best[i]], file=fout)

    elif mode == 'score':
        for i, j in zip(src_inds, trg_inds):
            s = score(x[i], y[j], x2y_mean[i], y2x_mean[j], margin)
            print(s, src_sents[i], trg_sents[j], sep='\t', file=fout)

    elif mode == 'mine':
        if verbose:
            print(' - mining for parallel data', file=sys.stderr)
        fwd_scores = score_candidates(x, y, x2y_ind, x2y_mean, y2x_mean, margin, verbose)
        bwd_scores = score_candidates(y, x, y2x_ind, y2x_mean, x2y_mean, margin, verbose)
        fwd_best = x2y_ind[np.arange(x.shape[0]), fwd_scores.argmax(axis=1)]
        bwd_best = y2x_ind[np.arange(y.shape[0]), bwd_scores.argmax(axis=1)]
        if verbose:
            print(' - writing alignments to {:s}'.format(output), file=sys.stderr)
            if threshold > 0:
                print(' - with threshold of {:f}'.format(threshold), file=sys.stderr)
        if retrieval == 'fwd':
            for i, j in enumerate(fwd_best):
                print(fwd_scores[i].max(), src_sents[i], trg_sents[j], sep='\t', file=fout)
        if retrieval == 'bwd':
            for j, i in enumerate(bwd_best):
                print(bwd_scores[j].max(), src_sents[i], trg_sents[j], sep='\t', file=fout)
        if retrieval == 'intersect':
            for i, j in enumerate(fwd_best):
                if bwd_best[j] == i:
                    print(fwd_scores[i].max(), src_sents[i], trg_sents[j], sep='\t', file=fout)
        if retrieval == 'max':
            indices = np.stack((np.concatenate((np.arange(x.shape[0]), bwd_best)),
                                np.concatenate((fwd_best, np.arange(y.shape[0])))), axis=1)
            scores = np.concatenate((fwd_scores.max(axis=1), bwd_scores.max(axis=1)))
            seen_src, seen_trg = set(), set()
            for i in np.argsort(-scores):
                src_ind, trg_ind = indices[i]
                if src_ind not in seen_src and trg_ind not in seen_trg:
                    seen_src.add(src_ind)
                    seen_trg.add(trg_ind)
                    if scores[i] > threshold:
                        print(src_doc_ind, trg_doc_ind, src_sents[src_ind], trg_sents[trg_ind], scores[i],
                              sep='\t', file=fout)

    if fout != sys.stdout:
        fout.close()


@click.command()
# embed params
@click.option("--src", help="Source sentences")
@click.option("--tgt", help="Target sentences")
@click.option("--offset", help="Sentence offset for document")
@click.option("--slang", "-sl", help="Source language code")
@click.option("--tlang", "-tl", help="Target language code")
@click.option("--token_slang", is_flag=True, default=True, help="Tokenize source sentences")
@click.option("--token_tlang", is_flag=True, default=True, help="Tokenize target sentences")
@click.option("--encoder", required=True, help="Encoder to be used")
@click.option("--bpe_codes", help="Apply BPE using specified codes")
@click.option("--buffer_size", type=int, default=10000, help="Buffer size (sentences)")
@click.option("--max_tokens", type=int, default=12000, help="Maximum number of tokens to process in a batch")
@click.option("--max_sentences", type=int, default=None, help="Maximum number of sentences to process in a batch")
@click.option("--enc_cpu/--enc_gpu", is_flag=True, default=True, help="Use GPU to encode sentences")
# mining params
@click.option("--encoding", default="utf8", help="Character encoding for input/output")
@click.option("--mode", type=click.Choice(["search", "score", "mine"]), default="mine", help="Execution mode")
@click.option("--neighborhood", "-k", type=int, default=4, help="Neighborhood size")
@click.option("--margin", type=click.Choice(["absolute", "distance", "ratio"]), default="ratio", help="Margin function")
@click.option("--retrieval", type=click.Choice(["fwd", "bwd", "max", "intersect"]), default="max", help="Retrieval strategy")
@click.option("--unify", is_flag=True, default=True, help="Unify texts")
@click.option("--knn_gpu/--knn_cpu", is_flag=True, default=True, help="Run kbb on all available GPUs")
@click.option("--stable", is_flag=True, default=False, help="Use stable merge sort instead of quick sort")
@click.option("--dim", type=int, default=1024, help="Embedding dimensionality")
@click.option("--threshold", type=float, default=0, help="Threshold on extracted bitexts")
@click.option("--verbose", is_flag=True, default=False, help="Detailed output")
@click.option("--output", help="Mining output")
def mine(src, tgt, offset, slang, tlang, token_slang, token_tlang,
         encoder, bpe_codes, buffer_size, max_tokens, max_sentences, enc_cpu,
         encoding, mode, neighborhood, margin, retrieval, unify, knn_gpu, stable, dim, threshold, verbose, output):

    buffer_size = max(buffer_size, 1)
    assert not max_sentences or max_sentences <= buffer_size, \
        '--max-sentences/--batch-size cannot be larger than --buffer-size'

    if verbose:
        print(' - Encoder: loading {}'.format(encoder))

    encoder = SentenceEncoder(encoder,
                              max_sentences=max_sentences,
                              max_tokens=max_tokens,
                              sort_kind='mergesort' if stable else 'quicksort',
                              cpu=enc_cpu)

    if offset:
        src_sents = [s.strip() for s in open(src)]
        tgt_sents = [t.strip() for t in open(tgt)]
        # The doc-index starts at 1 for use with the -n option of sed
        doc_offset = [(int(d[0])+1, d[1], d[2], int(d[3])+1, d[4], d[5])
                      for d in [line.strip().split() for line in open(offset)]]

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            src_tmpdir_path = tmpdir_path / slang
            tgt_tmpdir_path = tmpdir_path / tlang

            src_tmpdir_path.mkdir()
            tgt_tmpdir_path.mkdir()
            for s_ind, s_off, s_len, t_ind, t_off, t_len in doc_offset:
                src_txt = src_tmpdir_path / 'txt'
                tgt_txt = tgt_tmpdir_path / 'txt'

                with open(src_txt, "w") as fw:
                    print("\n".join(src_sents), file=fw)
                with open(tgt_txt, "w") as fw:
                    print("\n".join(tgt_sents), file=fw)

                src_embeddings = Embed(src_tmpdir_path.__str__(),
                                       src_txt.__str__(),
                                       encoder,
                                       slang if token_slang else "--",
                                       bpe_codes,
                                       buffer_size,
                                       verbose)
                tgt_embeddings = Embed(tgt_tmpdir_path.__str__(),
                                       tgt_txt.__str__(),
                                       encoder,
                                       tlang if token_tlang else "--",
                                       bpe_codes,
                                       buffer_size,
                                       verbose)

                # mine_output = tmpdir_path / "mine"

                Mine(s_ind + 1,
                     t_ind + 1,
                     src_txt.__str__(),
                     tgt_txt.__str__(),
                     encoding,
                     src_embeddings,
                     tgt_embeddings,
                     output,
                     # mine_output.__str__(),
                     unify,
                     mode,
                     retrieval,
                     margin,
                     neighborhood,
                     knn_gpu,
                     dim,
                     threshold,
                     verbose)

    else:
        src_embeddings = Embed(src, slang if token_slang else "--")
        src_embeddings = Embed(tgt, tlang if token_tlang else "--")


if __name__ == "__main__":
    mine()
