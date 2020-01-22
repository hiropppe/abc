#!/usr/bin/env python3

import click
import lxml.html
import re
import requests
import sys
import tldextract

from collections import Counter
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options


@click.command()
@click.option("--input_path", "-i", default=None, help="")
@click.option("--use_chrome", "-c", is_flag=True, default=False, help="")
def main(input_path, use_chrome):
    if use_chrome:
        print("Using Chrome", file=sys.stderr)

    if input_path:
        hosts = [line.strip() for line in open(input_path)]
    else:
        hosts = [host.strip() for host in sys.stdin]

    hosts = sorted(hosts, key=lambda x: tldextract.extract(x).domain)

    counter = Counter()
    for i, host in enumerate(hosts):
        print("{:d} {:s} >> ".format(i, host), file=sys.stderr, end="")

        seed, method, http_status, bytesize = resolve(host, use_chrome=use_chrome)
        if not seed and use_chrome:
            seed, method, http_status, bytesize = resolve(host, use_chrome=False)
            counter[method] += 1
        else:
            counter[method] += 1

        if seed and re.match("https?://\S+", seed):
            print(seed)

        print("[{:s}] {:s} {:d} ({:d})".format(method, seed if seed else "Discarded", bytesize, http_status), file=sys.stderr)

    print("input={:d}, canonical={:d}, www={:d}, discard={:d}".format(
        counter["i"], counter["c"], counter["w"], counter["d"]), file=sys.stderr)


def resolve(host, use_chrome=True):
    if use_chrome:
        chrome = Chrome()

    if '//' not in host:
        url = 'http://{:s}'.format(host)

    try:
        status_code = -1
        html = b''
        try:
            if use_chrome:
                test_url, html, status_code = chrome.get(url)
            else:
                test_url, html, status_code = requests_get(url)

            if not use_chrome:
                try:
                    root = lxml.html.fromstring(html)
                    canonical_url = root.cssselect("link[rel='canonical']")
                    if canonical_url:
                        canonical_url = canonical_url[0]
                        test_url = canonical_url.get("href")
                        test_url, html, status_code = requests_get(test_url)
                        if status_code == 200:
                            return test_url, "c", status_code, len(html)
                except:  # noqa
                    pass

            if status_code == 200:
                return test_url, "i", status_code, len(html)
        except:  # noqa
            pass

        ret = tldextract.extract(url)
        if not ret.subdomain:
            test_url = "http://" + ".".join(["www", ret.domain, ret.suffix])
            try:
                if use_chrome:
                    test_url, html, status_code = chrome.get(test_url)
                else:
                    test_url, html, status_code = requests_get(test_url)

                if status_code == 200:
                    return test_url, "w", status_code, len(html)
            except:  # noqa
                pass

        return None, "d", status_code, len(html)
    finally:
        if use_chrome:
            chrome.quit()


def requests_get(url):
    res = requests.get(url, timeout=(30, 60))
    return res.url, res.content, res.status_code


class Chrome:
    def __init__(self):
        self.options = Options()
        self.options.binary_location = '/usr/bin/google-chrome'
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--allow-running-insecure-content')
        self.options.add_argument('--disable-web-security')
        self.options.add_argument('--disable-desktop-notifications')
        self.options.add_argument("--disable-extensions")
        self.options.add_argument(
            '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36')
        self.options.add_argument('--lang=ja')
        self.options.add_argument('--blink-settings=imagesEnabled=false')

        self.driver = webdriver.Chrome(chrome_options=self.options)
        self.driver.set_page_load_timeout(60)
        self.driver.set_script_timeout(60)

    def __del__(self):
        self.quit()

    def get(self, url):
        self.driver.get(url)
        # Check current_url is accesible by requests
        _, content, status_code = requests_get(self.driver.current_url)
        return self.driver.current_url, content, status_code

    def quit(self):
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
        except:  # noqa
            pass


if __name__ == "__main__":
    main()
