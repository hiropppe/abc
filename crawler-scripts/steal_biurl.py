#!/usr/bin/env python3

import click
import lxml.html
import numpy as np
import os
import re
import requests
import time
import traceback
import urllib.parse

from collections import Counter
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.binary_location = '/usr/bin/google-chrome'
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-gpu')
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')
options.add_argument('--disable-web-security')
options.add_argument('--disable-desktop-notifications')
options.add_argument("--disable-extensions")
options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36')
options.add_argument('--lang=ja')
options.add_argument('--blink-settings=imagesEnabled=false')

root_url = "https://www.linguee.jp"

p_offset = None
p_daily_limit = None


@click.command()
@click.argument("output")
@click.option("--offset", type=int, default=1)
@click.option("--daylimit", type=int, default=50)
@click.option("--dic", default="ja-en")
def main(output, offset, daylimit, dic):
    global driver
    global p_offset, p_daily_limit
    global p_dic

    p_offset = offset
    p_daily_limit = daylimit

    try:
        driver = webdriver.Chrome(chrome_options=options)
        scrape(root_url, output, dic)
    finally:
        driver.quit()


def scrape(url, output, dic):
    global p_offset, p_daily_limit
    print(url)
    text, root = get(url)
    if dic == "ja-en":
        more_urls = re.findall(r"japanese-english/topjapanese/\d+-\d+.html", text)
    else:
        more_urls = re.findall(r"english-japanese/topenglish/\d+-\d+.html", text)
    if more_urls:
        for more_url in more_urls:
            try:
                more_url = root_url + "/" + more_url
                scrape(more_url, output, dic)
            except KeyboardInterrupt as e:
                raise e
            except Exception:
                traceback.print_exc()
            time.sleep(1)
    else:
        root = lxml.html.fromstring(text)
        word_numbers = [int(a.text.split(".")[0]) for a in root.cssselect("div#lingueecontent div table tr td")]
        word_urls = [a.get("href") for a in root.cssselect("div#lingueecontent div table tr td a")]
        assert len(word_numbers) == len(word_urls)
        for word_number, word_url in zip(word_numbers, word_urls):
            if word_number < p_offset:
                continue
            try:
                word_url = root_url + word_url
                biurl = find_hosts(word_number, word_url)
                if biurl:
                    with open(output, "a") as out:
                        for left, right in biurl:
                            print("{:s}\t{:s}\t{:s}".format(dic, left, right), file=out)
                else:
                    print("The request might be blocked. Sleep almost a day... zzz")
                    time.sleep(60*60*25)
            except KeyboardInterrupt as e:
                raise e
            except Exception:
                traceback.print_exc()


def find_hosts(word_number, word_url):
    print(word_number, urllib.parse.unquote(word_url))
    text, root = get(word_url)
    left_urls = [a.get("href").strip() for a in root.cssselect("table#result_table tr td.left div.wrap div.source_url a")]
    right_urls = [a.get("href").strip() for a in root.cssselect("table#result_table tr td.right2 div.wrap div.source_url a")]

    if len(left_urls) > 0 and len(left_urls) == len(right_urls):
        return zip(left_urls, right_urls)

    if len(left_urls) != len(right_urls):
        print("Length mismatch. {:s}".format(word_url), file=sys.stderr)


def get(url):
    min_wait = int(24*60*60*0.8/p_daily_limit)
    max_wait = int(24*60*60/p_daily_limit)

    time.sleep(np.random.randint(min_wait, max_wait))
    wait = WebDriverWait(driver, 60*3)
    driver.get(url)
    wait.until(ec.presence_of_all_elements_located)
    html = driver.page_source
    root = lxml.html.fromstring(html)
    return html, root


if __name__ == "__main__":
    main()
