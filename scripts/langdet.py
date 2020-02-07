def get_lang_detector(langid, model):
    if langid == "fastTexxt":
        det = get_fasttext_lang_detect(model)
    elif langid == "cld3":
        det = get_cld3_lang_detect()
    else:
        det = get_cld2_lang_detect()
    return det


def get_fasttext_lang_detect(model):
    import fasttext
    model = fasttext.load_model(model)

    def detect(text):
        pred = model.predict([text])[0]
        lang = pred[0][0][9:]
        return lang

    return detect


def get_cld3_lang_detect():
    import cld3
    model = cld3.LanguageIdentifier()

    def detect(text):
        lang, _, _, _ = model.get_language(text)
        return lang

    return detect


def get_cld2_lang_detect():
    import pycld2 as cld2

    def detect(text):
        _, _, langs = cld2.detect(text)
        return langs[0][1]

    return detect
