import re
import alcazar.bodytext
import logging

from bs4 import BeautifulSoup
from html.parser import HTMLParser as HTMLTokenizer
from selectolax.parser import HTMLParser


class SimpleParser(HTMLTokenizer):
    startNL = ["ul", "ol", "dl", "tr"]
    endNL = ["p", "div", "li", "dd", "dt", "th", "td", "h1", "h2", "h3", "h4", "h5", "h6"]
    selfNL = ["br"]
    noText = ["script", "noscript", "style"]
    lastTok = ""
    parsed = ""

    def handle_starttag(self, tag, attrs):
        if tag in self.startNL:
            self.parsed = self.parsed + "\n"
        self.lastTok = tag

    def handle_endtag(self, tag):
        if tag in self.endNL:
            self.parsed = self.parsed + "\n"

    def handle_startendtag(self, tag, attrs):
        if tag in self.selfNL:
            self.parsed = self.parsed + "\n"

    def handle_data(self, data):
        if self.lastTok not in self.noText:
            newdata = data.replace("\r\n", " ").replace("\n", " ")
            self.parsed = self.parsed + newdata

    def get_text(self):
        return self.parsed.strip() + "\n"


def get_text_extractor(parser):
    if parser == "alcazer":
        ext = extract_by_alcazer
    elif parser == "bs4":
        ext = extract_by_bs4
    elif parser == "modest":
        ext = extract_by_modest
    else:
        ext = get_simple_extractor()

    def extract(html):
        plaintext = ext(html)
        plaintext = re.sub(r"\r", "", plaintext.replace('\xa0', ' '))
        plaintext = re.sub(r" +", " ", plaintext)
        plaintext = re.sub(r"^\s+$", "\n", plaintext)
        plaintext = re.sub(r" *\n *", "\n", plaintext)
        plaintext = re.sub(r"\n+", "\n", plaintext).strip()
        return plaintext

    return extract


def extract_by_alcazer(text):
    btext = alcazar.bodytext.parse_article(text)
    if text.body_text:
        plaintext = btext.body_text
    else:
        plaintext = ""
    return plaintext


def extract_by_bs4(text):
    try:
        soup = BeautifulSoup(text, "lxml")
    except Exception as ex:
        logging.info("Exception ocurred when processing with BeautifulSoup")
        raise ex

    for script in soup(["script", "style", "img"]):
        script.extract()  # rip it out

    plaintext = soup.get_text()
    return plaintext


def extract_by_modest(text):
    try:
        tree = HTMLParser(text)
    except:  # noqa
        logging.warn("Tree structure issues in HTML/XML. Ignoring this document")
        raise ValueError()

    for tag in tree.css('script'):
        tag.decompose()
    for tag in tree.css('style'):
        tag.decompose()
    for tag in tree.css('img'):
        tag.decompose()

    if tree.body is None:
        logging.warn("Body is empty. Ignoring this document")
        raise ValueError()

    plaintext = tree.body.text(separator='\n')
    return plaintext


def get_simple_extractor():
    parser = SimpleParser()

    def extract(text):
        parser.feed(text)
        plaintext = parser.get_text()
        return plaintext

    return extract
