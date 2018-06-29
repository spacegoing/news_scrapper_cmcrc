# -*- coding: utf-8 -*-
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk, string
from nltk.stem.porter import PorterStemmer


def stem_tokens(tokens, stemmer):
  stemmed = []
  for item in tokens:
    stemmed.append(stemmer.stem(item))
  return stemmed


def tokenize(text):
  tokens = nltk.word_tokenize(text)
  # use porterstemmer
  stemmer = PorterStemmer()
  stems = stem_tokens(tokens, stemmer)
  return stems


def text_filter(text):

  def filter_punct(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.lower().translate(translator)

  def filter_custom(text):
    ind = text.find('-')
    return text[ind + 1:]

  return filter_punct(filter_custom(text))


filename = "result_asx_sgx_johannesburg_istanbul_sao_paulo_lse_nasdaq_2018-05-03_2018-06-01.csv"

news_df = pd.read_csv(filename, index_col=None)
news_df['Headline'] = news_df['Headline'].apply(text_filter)

tfidf = TfidfVectorizer(tokenizer=tokenize, stop_words='english')
tfs = tfidf.fit_transform(news_df['Headline'])
cos_mat = (tfs * tfs.T).A


# inter day similarity
