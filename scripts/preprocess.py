#!/usr/bin/env python3

import cchardet
import click
import logging
import lzma
import sys

from functools import partial
from typing import Iterable, Iterator, List, Optional
from warcio.archiveiterator import ArchiveIterator


log = logging.getLogger(__name__).info


@click.command()
@click.option("--input", "-i", help="Input WARC file")
@click.option("--output", "-o", help="")
@click.option("--xzlang", is_flag=True, default=True, help="")
@click.option("--boilerpipe", is_flag=True, default=False, help="")
@click.option("--langid", type=click.Choice(["cld2", "cld3", "fastText"]), default="cld2")
@click.option("--langid_model", default="./model/fastText/lid.176.bin", help="fastText LID model path")
def main(input,
         output,
         xzlang,
         boilerpipe,
         langid,
         langid_model):
    for doc in WarcReader(input):
        print(doc["html"])


class WarcReader(Iterable[dict]):
    def __init__(self, input):
        self.input = input

    def __iter__(self) -> Iterator[dict]:
        if self.input[-3:] == ".xz":
            warc_open = partial(lzma.open, mode="r")
        elif self.input[-3:] == ".gz":
            warc_open = partial(open, mode="rb")
        else:
            warc_open = open

        with warc_open(self.input) as input_stream:
            for record in ArchiveIterator(input_stream):
                try:
                    data = self.validate_record(record)
                except ValueError:
                    continue

                payload = record.content_stream().read()

                # We convert into UTF8 first of all
                encoding, html = convert_encoding(payload)
                log("Processing document: " + data['url'])
                if encoding is None:
                    log("Encoding of document " + data['url'] + " could not be identified")
                    continue

                if len(html) == 0:
                    continue

                data['date'] = record.rec_headers.get_header('WARC-Date')
                data['record_id'] = record.rec_headers.get_header('WARC-Record-ID')
                data['encoding'] = encoding
                data['html'] = html

                yield data

    def validate_record(self, record) -> dict:
        if record.rec_type not in ('response', 'resource'):
            raise ValueError()

        target_uri = record.rec_headers.get_header("WARC-Target-URI")
        if target_uri[0] == '<' and target_uri[-1] == '>':
            url = target_uri[1:-1]
        else:
            url = target_uri

        if url == "unknown":
            raise ValueError()

        url = url.lower().replace('\t', ' ')
        if (url[-4:] == ".gif" or
                url[-4:] == ".jpg" or
                url[-5:] == ".jpeg" or
                url[-4:] == ".png" or
                url[-4:] == ".css" or
                url[-3:] == ".js" or
                url[-4:] == ".mp3" or
                url[-4:] == ".mp4" or
                url[-4:] == ".ogg" or
                url[-5:] == ".midi" or
                url[-4:] == ".swf"):
            raise ValueError()

        # Ignore robots.txt when processing records
        if url[-11:] == "/robots.txt":
            raise ValueError("/robots.txt")

        return {
            "url": url,
        }


def convert_encoding(data):
    encoding = cchardet.detect(data)['encoding']
    if encoding is None:
        encoding = "utf-8"
    if len(data) > 0:
        # We convert, even if the text is detected to be UTF8 so, if it is an error and conversion fails,
        # the error is caught here
        for enc in [encoding, 'utf-8', 'iso-8859-1', 'windows‑1252']:
            try:
                return enc, data.decode(enc).strip()
            except:  # noqa
                pass
    return None, ''


if __name__ == "__main__":
    main()
