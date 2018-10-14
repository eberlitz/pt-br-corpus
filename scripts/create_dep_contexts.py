#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import glob
from collections import Counter

re_extract_context = re.compile(
    r'^(.*?) +.*?(@.*?)\s+#(\d+)->(\d+)', re.MULTILINE | re.UNICODE)
re_remove_especial = re.compile(r'^\$.*\s', re.MULTILINE | re.UNICODE)
remove_doc_tags = re.compile(r'<\/?ß>\s+', re.MULTILINE | re.UNICODE)


def regex_to_contexts(text):
    text = text.lower()
    text = remove_doc_tags.sub(r'', text)
    text = re_remove_especial.sub(r'', text)
    text = re_extract_context.sub(r'\3|\1|\2|\4', text)
    return text


def extract_context_from_doc_text(text):
    words = {}
    for line in regex_to_contexts(doc).split('\n'):
        tokens = line.split('|')
        if len(tokens) == 4:
            num = tokens[0]
            word = tokens[1]
            subwords = word.split('=')
            if len(subwords) > 1:
                subwords.append(word)
            contexts = tokens[2].replace('@', '').split(' ')
            ref = tokens[3]
            words[num] = (num, subwords, contexts, ref)
    for key in words:
        (num, subwords, contexts, ref) = words[key]
        if (ref in words):
            (_, ref_words, _, _) = words[ref]
            for word in subwords:
                for context in contexts:
                    for ref_word in ref_words:
                        yield (word, '{}_{}'.format(context, ref_word))
                        # print('{} {}_{}'.format(word, context, ref_word))



def read_wiki_documents(dirname):
    filepaths = glob.glob(dirname+'/**/wiki_*')
    filepaths.sort()
    for fp in filepaths:
        with open(fp, 'r') as input:
            doc = ''
            for line in input:
                doc += line
                if (line.startswith('<ß>')):
                    doc = line
                elif line.startswith('</ß>'):
                    yield doc



cv = Counter()
wv = Counter()

for doc in read_wiki_documents('../data/ptwiki-articles-palavras/'):
    print('doc')
    for (word, context) in extract_context_from_doc_text(doc):
        print(word)
        wv.update([word])
        cv.update([context])

print(cv)
print(wv)