# Generating the corpus

1. Download the Wikipedia pt-br articles dump:

```sh
curl https://dumps.wikimedia.org/ptwiki/latest/ptwiki-latest-pages-articles-multistream-index.txt.bz2 --create-dirs -o data/ptwiki-latest-pages-articles-multistream-index.txt.bz2
curl https://dumps.wikimedia.org/ptwiki/latest/ptwiki-latest-pages-articles-multistream.xml.bz2 --create-dirs -o data/ptwiki-latest-pages-articles-multistream.xml.bz2
```

2. The wikipedia dump will have all articles in the wiki text format that is like a markdown with special tokens. So in order to get only the text we need to transform the wiki format to raw text. We could use the pythons `gensim.corpora.WikiCorpus` but its tokenizer is not so good for Portuguese. So I ended up using the `wikiextractor` and then I cleanup the text myself using another script. So, clone and execute the `wikiextractor` to transform the xml data into text:

```sh
git clone https://github.com/attardi/wikiextractor.git
cd ./wikiextractor
python ./WikiExtractor.py --no-templates -o ../data/ptwiki-articles-text/ -b 10M -c ../data/ptwiki-latest-pages-articles-multistream.xml.bz2
cd ..
```

This process will generate multiple compressed files of 10MB of wiki articles texts in the following format:

```html
<doc id="2" url="http://it.wikipedia.org/wiki/Harmonium">
Harmonium.
L'harmonium è uno strumento musicale azionato con una tastiera, detta manuale.
Sono stati costruiti anche alcuni harmonium con due manuali.
...
</doc>
```

At the time of writing there was 1000400 documents in the ptwiki-dump. =]


3. Now that we have the wikipedia texts, we can start the pre-processing of the files.

```sh
python scripts/preprocess.py ./data/ptwiki-articles-text/ -o ./data/ptwiki-articles-text-cleaned
```

This script will do the following:

- Breaks into multiple sentences using nltk.data.load('tokenizers/punkt/portuguese.pickle').
- It will not change the case. (Later I'll use a POS parser that have a better accuracy if I maintain this)
- Remove sentences with less than 4 words.
- It will allow abbreviations, like 'Dr.'.
- It will keep words with '-', like 'guarda-chuva'.
- All emails are mapped to a EMAIL token.
- All numbers are mapped to 0 token.
- All urls are mapped to URL token.
- Different quotes are standardized.
- Different hiphen are standardized.
- HTML strings are removed.
- All text between brackets are removed.

These steps will generate a pt-BR corpus with:

- 1.6GB
- 9896520 sentences
- 251193592 tokens
- 3137040 unique tokens
