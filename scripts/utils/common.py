try:
    import lzma
except ImportError:
    from backports import lzma
#  This file is part of Bitextor.
#
#  Bitextor is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Bitextor is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Bitextor.  If not, see <https://www.gnu.org/licenses/>.

import gzip
from contextlib import contextmanager


@contextmanager
def open_xz_or_gzip_or_plain(file_path):
    def decode_text(file_handler):
        for line in file_handler:
            yield line.decode('utf-8')

    f = None
    try:
        if file_path[-3:] == ".gz":
            f = gzip.open(file_path, 'rb')
            yield decode_text(f)
        elif file_path[-3:] == ".xz":
            f = lzma.open(file_path, 'rb')
            yield decode_text(f)
        else:
            f = open(file_path, 'r')
            yield f

    except Exception:
        raise Exception("Error occured while loading a file!")

    finally:
        if f:
            f.close()


def build_mappings(file_path_from, file_path_to, column=None, dem='\t'):
    mapping = {}

    def next_or_next_in_column(handler):
        if not column:
            return next(handler, None)

        text = next(handler, None)
        if text:
            return text.split(dem)[column]

        return text

    with open_xz_or_gzip_or_plain(file_path_from) as f_from, open_xz_or_gzip_or_plain(file_path_to) as f_to:
        line_from = next_or_next_in_column(f_from)
        line_to = next_or_next_in_column(f_to)

        while line_from and line_to:
            line_from = line_from.strip()
            mapping[line_from] = line_to.strip()

            line_from = next_or_next_in_column(f_from)
            line_to = next_or_next_in_column(f_to)

    return mapping


def check_lengths(file_path_from, file_path_to, throw=True):
    f1_lines = 0
    f2_lines = 0
    with open_xz_or_gzip_or_plain(file_path_from) as f:
        for _ in f:
            f1_lines += 1

    with open_xz_or_gzip_or_plain(file_path_to) as f:
        for _ in f:
            f2_lines += 1

    if throw and f1_lines != f2_lines:
        raise Exception("Files must have the same number of lines!\
                            {0}: {1}, {2}: {3}".format(file_path_from, f1_lines, file_path_to, f2_lines))

    return f1_lines == f2_lines
