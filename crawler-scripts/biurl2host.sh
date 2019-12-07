#!/bin/bash
cat linguee_biurl | cut -f2 | python3 -c "import sys; from urllib.parse import urlparse; print('\n'.join([urlparse(url.strip()).netloc for url in sys.stdin]))" > .linguee_biurl.host.tmp
cat linguee_biurl | cut -f3 | python3 -c "import sys; from urllib.parse import urlparse; print('\n'.join([urlparse(url.strip()).netloc for url in sys.stdin]))" >> .linguee_biurl.host.tmp
sort .linguee_biurl.host.tmp | uniq > linguee_biurl.host
rm .linguee_biurl.host.tmp
