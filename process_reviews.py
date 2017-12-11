#!/usr/bin/python3.5
import re
import csv
import string
import hashlib
from py2neo import Graph
from nltk import tokenize
from nltk.tokenize import WordPunctTokenizer

graphdb = Graph('http://neo4j:neo4j@localhost:7474/db/data')
word_punct_tokenizer = WordPunctTokenizer()
regex = re.compile('[%s]' % re.escape(string.punctuation))
tx = graphdb.begin()

INSERT_QUERY = """
    WITH split(tolower({sentence}), " ") AS words
    WITH [w in words WHERE NOT w IN ["the","and","i", "it", "to"]] AS text
    UNWIND range(0,size(text)-2) AS i
    MERGE (w1:Word {word: text[i]})
    ON CREATE SET w1.count = 1 ON MATCH SET w1.count = w1.count + 1
    MERGE (w2:Word {word: text[i+1]})
    ON CREATE SET w2.count = 1 ON MATCH SET w2.count = w2.count + 1
    MERGE (w1)-[r:NEXT]->(w2)
      ON CREATE SET r.count = 1
      ON MATCH SET r.count = r.count + 1;
"""
FREQ_QUERY = """
    MATCH (w:Word)
    RETURN w.word AS Word, w.count AS Word_count
    ORDER BY w.count DESC LIMIT 10
"""
FREQ_PAIRS_QUERY = """
    MATCH (w1:Word)-[r:NEXT]->(w2:Word)
    RETURN [w1.word, w2.word] AS Word_pair, r.count AS Word_count
    ORDER BY r.count DESC LIMIT 10
"""
SUMMARIZE_QUERY = """
MATCH p=(:Word { word: 'breakfast' } )-[r:NEXT*1..4]->(:Word) WITH p
WITH reduce(s = 0, x IN relationships(p) | s + x.count) AS total, p
WITH nodes(p) AS text, 1.0*total/size(nodes(p)) AS weight
RETURN extract(x IN text | x.word) AS phrase, weight ORDER BY weight DESC LIMIT 10
"""


def paragraph_to_sentence(paragraph: str) -> list:
    return paragraph.split(".")
    # return tokenize.sent_tokenize(paragraph)


def clean_sentence(sentence: str):
    sentence = sentence.lower()
    sentence = sentence.strip()
    sentence = regex.sub("", sentence)
    return sentence
    # words = re.findall('[A-Za-z0-9\']+', sentence)
    # return " ".join(words)


def read_reviews():
    with open('7282_1.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        return [(row['reviews.text'], row['name']) for row in reader]


def insert_reviews():
    tx = graphdb.begin()
    ratings = read_reviews()
    count = 0
    for review, venue_name in ratings:
        if venue_name != 'Best Western Plus Waterville Grand Hotel':
            continue
        for sentence in paragraph_to_sentence(review):
            params = {'sentence': clean_sentence(sentence)}
            tx.append(INSERT_QUERY, params)
            tx.process()
            count += 1
        if count > 100:
            tx.commit()
            tx = graphdb.begin()
            count = 0


def main():
    # insert_reviews()
    print(tx.run(FREQ_QUERY).dump())
    print(tx.run(FREQ_PAIRS_QUERY).dump())
    print(tx.run(SUMMARIZE_QUERY).dump())


if __name__ == '__main__':
    main()
