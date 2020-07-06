import click
import gzip
import sys
import tldextract

from collections import defaultdict
from pathlib import Path


@click.command()
@click.argument("seeds_path")
@click.option('--min_size', '-m', default=3, help='minimum warc size in MB')
def main(seeds_path, min_size):
    CRAWLER = "heritrix"

    def create_seed_dict(seeds):
        dic = defaultdict(list)
        for seed in seeds:
            ext = tldextract.extract(seed)
            key = ".".join([ext.subdomain, ext.domain, ext.suffix]).lstrip(".")
            dic[key].append(seed)
        return dic

    crawled_hosts = []
    for line in sys.stdin:
        size, path = line.split()
        size = int(size)
        if size >= min_size*1024:
            dirname = path.split('/')[-2]
            print(f'{dirname}\t{size/1024.0:.2f}M', file=sys.stderr)
            crawled_hosts.append(dirname)

    crawled_hosts = set(crawled_hosts)

    print(f"read hosts from warc dir={len(crawled_hosts):d}", file=sys.stderr)

    with gzip.open(seeds_path, "rt") as f:
        seeds = set(f.read().splitlines())

    seed_dict = create_seed_dict(seeds)
    seed_hosts = list(seed_dict.keys())

    hosts_to_crawl = list(set(seed_hosts) - crawled_hosts)

    print("#Total hosts=" + str(len(seed_hosts)), file=sys.stderr)
    print("#Hosts to crawl=" + str(len(hosts_to_crawl)), file=sys.stderr)

    for key in hosts_to_crawl:
        for seed in seed_dict[key]:
            print(seed, file=sys.stdout)


if __name__ == '__main__':
    main()
