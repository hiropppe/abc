#!/usr/bin/env python3

import click
import faiss
import numpy as np
import os
import re
import sys
import tempfile
import time
import torch
import torch.nn as nn

from collections import namedtuple

# get environment
assert os.environ.get('LASER'), 'Please set the enviornment variable LASER'
LASER = os.environ['LASER']

sys.path.append(LASER + '/source')
sys.path.append(LASER + '/source/lib')
sys.path.append(LASER + '/source/tools')
from embed import SentenceEncoder, EncodeLoad, EncodeFile, EmbedLoad  # noqa
from text_processing import Token, BPEfastApply  # noqa


SPACE_NORMALIZER = re.compile("\s+")
Batch = namedtuple('Batch', 'srcs tokens lengths')


def buffered_read(fp, buffer_size):
    buffer = []
    for src_str in fp:
        buffer.append(src_str.strip())
        if len(buffer) >= buffer_size:
            yield buffer
            buffer = []

    if len(buffer) > 0:
        yield buffer


def buffered_arange(max):
    if not hasattr(buffered_arange, 'buf'):
        buffered_arange.buf = torch.LongTensor()
    if max > buffered_arange.buf.numel():
        torch.arange(max, out=buffered_arange.buf)
    return buffered_arange.buf[:max]


# TODO Do proper padding from the beginning
def convert_padding_direction(src_tokens, padding_idx, right_to_left=False, left_to_right=False):
    assert right_to_left ^ left_to_right
    pad_mask = src_tokens.eq(padding_idx)
    if not pad_mask.any():
        # no padding, return early
        return src_tokens
    if left_to_right and not pad_mask[:, 0].any():
        # already right padded
        return src_tokens
    if right_to_left and not pad_mask[:, -1].any():
        # already left padded
        return src_tokens
    max_len = src_tokens.size(1)
    range = buffered_arange(max_len).type_as(src_tokens).expand_as(src_tokens)
    num_pads = pad_mask.long().sum(dim=1, keepdim=True)
    if right_to_left:
        index = torch.remainder(range - num_pads, max_len)
    else:
        index = torch.remainder(range + num_pads, max_len)
    return src_tokens.gather(1, index)


class Encoder(nn.Module):
    def __init__(
            self, num_embeddings, padding_idx, embed_dim=320, hidden_size=512, num_layers=1, bidirectional=False,
            left_pad=True, padding_value=0.
    ):
        super().__init__()

        self.num_layers = num_layers
        self.bidirectional = bidirectional
        self.hidden_size = hidden_size

        self.padding_idx = padding_idx
        self.embed_tokens = nn.Embedding(num_embeddings, embed_dim, padding_idx=self.padding_idx)

        self.lstm = nn.LSTM(
            input_size=embed_dim,
            hidden_size=hidden_size,
            num_layers=num_layers,
            bidirectional=bidirectional,
        )
        self.left_pad = left_pad
        self.padding_value = padding_value

        self.output_units = hidden_size
        if bidirectional:
            self.output_units *= 2

    def forward(self, src_tokens, src_lengths):
        if self.left_pad:
            # convert left-padding to right-padding
            src_tokens = convert_padding_direction(
                src_tokens,
                self.padding_idx,
                left_to_right=True,
            )

        bsz, seqlen = src_tokens.size()

        # embed tokens
        x = self.embed_tokens(src_tokens)

        # B x T x C -> T x B x C
        x = x.transpose(0, 1)

        # pack embedded source tokens into a PackedSequence
        packed_x = nn.utils.rnn.pack_padded_sequence(x, src_lengths.data.tolist())

        # apply LSTM
        if self.bidirectional:
            state_size = 2 * self.num_layers, bsz, self.hidden_size
        else:
            state_size = self.num_layers, bsz, self.hidden_size
        h0 = x.data.new(*state_size).zero_()
        c0 = x.data.new(*state_size).zero_()
        packed_outs, (final_hiddens, final_cells) = self.lstm(packed_x, (h0, c0))

        # unpack outputs and apply dropout
        x, _ = nn.utils.rnn.pad_packed_sequence(packed_outs, padding_value=self.padding_value)
        assert list(x.size()) == [seqlen, bsz, self.output_units]

        if self.bidirectional:
            def combine_bidir(outs):
                return torch.cat([
                    torch.cat([outs[2 * i], outs[2 * i + 1]], dim=0).view(1, bsz, self.output_units)
                    for i in range(self.num_layers)
                ], dim=0)

            final_hiddens = combine_bidir(final_hiddens)
            final_cells = combine_bidir(final_cells)

        encoder_padding_mask = src_tokens.eq(self.padding_idx).t()

        # Set padded outputs to -inf so they are not selected by max-pooling
        padding_mask = src_tokens.eq(self.padding_idx).t().unsqueeze(-1)
        if padding_mask.any():
            x = x.float().masked_fill_(padding_mask, float('-inf')).type_as(x)

        # Build the sentence embedding by max-pooling over the encoder outputs
        sentemb = x.max(dim=0)[0]

        return {
            'sentemb': sentemb,
            'encoder_out': (x, final_hiddens, final_cells),
            'encoder_padding_mask': encoder_padding_mask if encoder_padding_mask.any() else None
        }


def EncodeTime(t):
    t = int(time.time() - t)
    if t < 1000:
        print(' in {:d}s'.format(t))
    else:
        print(' in {:d}m{:d}s'.format(t // 60, t % 60))


# Encode sentences (existing file pointers)
def EncodeFilep(encoder, inp_file, out_file, buffer_size=10000, verbose=False):
    n = 0
    t = time.time()
    for sentences in buffered_read(inp_file, buffer_size):
        encoder.encode_sentences(sentences).tofile(out_file)
        n += len(sentences)
        if verbose and n % 10000 == 0:
            print('\r - Encoder: {:d} sentences'.format(n), end='')
    if verbose:
        print('\r - Encoder: {:d} sentences'.format(n), end='')
        EncodeTime(t)


# Get memory mapped embeddings
def EmbedMmap(fname, dim=1024, dtype=np.float32, verbose=False):
    nbex = int(os.path.getsize(fname) / dim / np.dtype(dtype).itemsize)
    E = np.memmap(fname, mode='r', dtype=dtype, shape=(nbex, dim))
    if verbose:
        print(' - embeddings on disk: {:s} {:d} x {:d}'.format(fname, nbex, dim))
    return E


def embed(sents, token_lang):
    with tempfile.TemporaryDirectory() as tmpdir:
        ifname = ''  # stdin will be used
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


@click.command()
@click.option("--src", help="Source sentences")
@click.option("--tgt", help="Target sentences")
@click.option("--offset", help="Sentence offset for document")
@click.option("--slang", help="Source language code")
@click.option("--tlang", help="Target language code")
@click.option("--token_slang", is_flag=True, default=True, help="Tokenize source sentences")
@click.option("--token_tlang", is_flag=True, default=True, help="Tokenize target sentences")
@click.option("--encoder", required=True, help="Encoder to be used")
@click.option("--bpe_codes", help="Apply BPE using specified codes")
@click.option("--buffer_size", type=int, default=10000, help="Buffer size (sentences)")
@click.option("--max_tokens", type=int, default=12000, help="Maximum number of tokens to process in a batch")
@click.option("--max_sentences", type=int, default=None, help="Maximum number of sentences to process in a batch")
@click.option("--enc_cpu", is_flag=True, default=True, help="Use GPU to encode sentences")
@click.option("--mode", type=click.Choice(["search", "score", "mine"]), default="mine", help="Execution mode")
@click.option("--neighborhood", "-k", type=int, default=4, help="Neighborhood size")
@click.option("--margin", type=click.Choice(["absolute", "distance", "ratio"]), default="ratio", help="Margin function")
@click.option("--retrieval", type=click.Choice(["fwd", "bwd", "max", "intersect"]), default="max", help="Retrieval strategy")
@click.option("--unify", is_flag=True, default=False, help="Unify texts")
@click.option("--knn_gpu", is_flag=True, default=False, help="Run kbb on all available GPUs")
@click.option("--stable", is_flag=True, default=False, help="Use stable merge sort instead of quick sort")
@click.option("--verbose", is_flag=True, default=False, help="Detailed output")
def mine(input, offset, slang, tlang, token_slang, token_tlang,
         encoder, bpe_codes, buffer_size, max_tokens, max_sentences, enc_cpu,
         mode, neighborhood, margin, retrieval, unify, knn_gpu, stable, verbose):

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

    src_sents = [s.strip() for s in open(src)]
    tgt_sents = [t.strip() for t in open(tgt)]

    if offset:
        doc_offset = [(d[2], d[2], d[4], d[5]) for d in [line.strip().split() for line in open(offset)]
        for soff, s_len, toff, t_len in doc_offset:
            src_embeds= embed(src_sents[soff: soff+slen], slang if token_slang else "--")
            tgt_embeds= embed(tgt_sents[toff: toff+tlen], tlang if token_tlang else "--")


if __name__ == "__main__":
    mine()
