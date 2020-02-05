#!/usr/bin/env python3

import click
import lzma

from warcio.archiveiterator import ArchiveIterator


@click.command()
@click.option("--input", "-i", help="")
@click.option("--output", "-o", help="")
def main(input, output):
    if input[-3:] == ".xz":
        warc_iter = ArchiveIterator(lzma.open(input, "r"))
    elif input[-3:] == ".gz":
        warc_iter = ArchiveIterator(open(input, "rb"))
    elif input is not None:
        warc_iter = ArchiveIterator(open(input), "r")
    else:
        warc_iter = ArchiveIterator(sys.stdin.buffer)

    for record in warc_iter:
        process_record(record)


def process_record(record):
    # Initial checks
    if record.rec_type != 'response' and record.rec_type != 'resource':
        continue
    if record.rec_headers.get_header('WARC-Target-URI')[0] == '<' and record.rec_headers.get_header('WARC-Target-URI')[-1] == '>':
        url = record.rec_headers.get_header('WARC-Target-URI')[1:-1]
    else:
        url = record.rec_headers.get_header('WARC-Target-URI')
    if url == "unknown":
        logging.info("Skipping page with unknown URL")
        continue
    url = url.lower()
    url = url.replace('\t', ' ')
    if url[-4:] == ".gif" or url[-4:] == ".jpg" or url[-5:] == ".jpeg" or url[-4:] == ".png" or url[-4:] == ".css" or url[-3:] == ".js" or url[-4:] == ".mp3" or url[-4:] == ".mp4" or url[-4:] == ".ogg" or url[-5:] == ".midi" or url[-4:] == ".swf":
        continue

    # Ignore robots.txt when processing records
    if url[-11:] == "/robots.txt":
        continue

    payload = record.content_stream().read()

    # We convert into UTF8 first of all
    orig_encoding, text = convert_encoding(payload)
    logging.info("Processing document: " + url)
    if orig_encoding is None:
        logging.info("Encoding of document " + url + " could not be identified")
        continue

    date = record.rec_headers.get_header('WARC-Date')
    recordId = record.rec_headers.get_header('WARC-Record-ID')

    if len(text.strip()) == 0:
        continue

    # lang id
    logging.info(url + ": detecting language")
    lang = ""

    if options.langid == "cld3":
        lang = guess_lang_from_data3(cld3model, text)
    else:
        lang = guess_lang_from_data2(text)

    if (len(languages) > 0 and lang not in languages) or (lang in banned):
        logging.info("Language of document " + url + ": " +
                     lang + ". Not among searched languages.")
        continue

    if lang == "un":
        logging.info("Language of document " + url + " could not be identified")
        continue

    if not options.xzlang and lang not in files_dict:
        if not os.path.exists(options.outDir + "/" + lang):
            os.makedirs(options.outDir + "/" + lang)
        urlFile = lzma.open(options.outDir + "/" + lang + "/url.xz", "w")
        encodingFile = lzma.open(options.outDir + "/" + lang + "/encoding.xz", "w")
        mimeFile = lzma.open(options.outDir + "/" + lang + "/mime.xz", "w")
        normHtmlFile = lzma.open(options.outDir + "/" + lang + "/normalized_html.xz", "w")
        plainTextFile = lzma.open(options.outDir + "/" + lang + "/plain_text.xz", "w")
        if options.boilerpipe:
            deboilFile = lzma.open(options.outDir + "/" + lang + "/" + "deboilerplate_html.xz", "w")
            files_dict[lang] = {"urlFile": urlFile, "encodingFile": encodingFile, "mimeFile": mimeFile,
                                "normHtmlFile": normHtmlFile, "plainTextFile": plainTextFile, "deboilFile": deboilFile}
        else:
            if not os.path.exists(options.outDir + "/" + lang + "/" + "deboilerplate_html.xz") and not os.path.islink(options.outDir + "/" + lang + "/" + "deboilerplate_html.xz"):
                os.symlink("normalized_html.xz", options.outDir +
                           "/" + lang + "/" + "deboilerplate_html.xz")
            files_dict[lang] = {"urlFile": urlFile, "encodingFile": encodingFile,
                                "mimeFile": mimeFile, "normHtmlFile": normHtmlFile, "plainTextFile": plainTextFile}

    # If enabled, remove boilerplate HTML
    if options.boilerpipe:
        logging.info(url + ": deboiling html")
        extractor = ExtrB(extractor='ArticleExtractor', html=text)
        deboiled = str(extractor.getHTML())
    else:
        deboiled = text

    if options.neologdn:
        normed_text = neologdn.normalize(deboiled)
    else:
        normed_text = deboiled

    # We compute a hash on the HTML (either normalized one or after boilerpipe if enabled):
    # if we get duplicate files we discard them
    html_hash = mmh3.hash(normed_text, signed=False)
    # checking for duplicate content (duplicates are discarded)
    if html_hash in seen_html:
        logging.info("Repeated file:\t" + url)
        continue

    # get text with Alcazar library
    if options.parser == "alcazar":
        logging.info(url + ": Getting text with Alcazar")
        btext = alcazar.bodytext.parse_article(deboiled)
        if btext.body_text:
            plaintext = btext.body_text
        else:
            plaintext = ""

    # or get text with beautifulsoup
    elif options.parser == "bs4":
        logging.info(url + ": Getting text with BeautifulSoup")
        try:
            soup = BeautifulSoup(deboiled, "lxml")
        except Exception as ex:
            logging.info("Exception ocurred when processing " + url + " with BeautifulSoup")
            continue

        for script in soup(["script", "style", "img"]):
            script.extract()  # rip it out
        plaintext = soup.get_text()

    # or get text with 'modest' library
    elif options.parser == "modest":
        logging.info(url + ": Getting text with modest (selectolax)")
        try:
            tree = HTMLParser(deboiled)
        except:
            logging.info("Tree structure issues in HTML/XML. Ignoring this document")
            continue
        for tag in tree.css('script'):
            tag.decompose()
        for tag in tree.css('style'):
            tag.decompose()
        for tag in tree.css('img'):
            tag.decompose()
        if tree.body is None:
            logging.info("Body is empty. Ignoring this document")
            continue
        plaintext = tree.body.text(separator='\n')

    # or use an HTML tokenizer
    else:
        logging.info(url + ": Getting text with HTML tokenizer")
        parser = SimpleParser()
        parser.feed(text)
        plaintext = parser.get_text()

    plaintext = re.sub(r"\n+", "\n", re.sub(r" *\n *", "\n", re.sub(r"^\s+$", "\n",
                                                                    re.sub(r" +", " ", re.sub(r"\r", "", plaintext.replace(u'\xa0', u' ')))))).strip()
    plaintext_hash = mmh3.hash(plaintext, signed=False)

    if plaintext_hash in seen_plain_text or plaintext_hash in previous_crawl_hashes:
        logging.info("Repeated plain text file:\t" + url)
        continue

    if len(plaintext) > 0:

        seen_html.add(html_hash)
        seen_plain_text.add(plaintext_hash)
        # Guessing MIME of the file (checked on original content)
        logging.info(url + ": Getting mime")
        mime = magic.from_buffer(text, mime=True)

        if not options.xzlang:
            files_dict[lang]["mimeFile"].write(mime.encode() + b"\n")
            files_dict[lang]["urlFile"].write(url.encode() + b"\n")
            files_dict[lang]["encodingFile"].write(orig_encoding.encode() + b"\n")

            b64norm = base64.b64encode(text.encode())
            files_dict[lang]["normHtmlFile"].write(b64norm + b"\n")

            if options.boilerpipe:
                b64deboil = base64.b64encode(deboiled.encode())
                files_dict[lang]["deboilFile"].write(b64deboil + b"\n")

            b64text = base64.b64encode(html.unescape(plaintext).encode())
            files_dict[lang]["plainTextFile"].write(b64text + b"\n")
        # append to language specific file
        else:
            langfile = lzma.open(options.outDir + "/" + lang, mode="a", format=lzma.FORMAT_XZ)
            header = "Content-Location: " + url + "\n"
            header += "Content-Type: " + mime + "\n"
            header += "Content-Language: " + lang + "\n"
            header += "Content-Length: " + str(len(plaintext)) + "\n"
            header += "Date: " + date + "\n"
            header += "X-WARC-Record-ID: " + recordId + "\n"
            header += "X-WARC-Filename: " + options.input + "\n"
            langfile.write(header.encode())
            langfile.write(b"\n")
            langfile.write(plaintext.encode())
            langfile.write(b"\n")
            langfile.close()

        if options.outputHash:
            plainTextHashFile.write(str(plaintext_hash).encode() + b"\n")

if not options.xzlang:
    for lang in files_dict:


if __name__ == "__main__":
    main()
