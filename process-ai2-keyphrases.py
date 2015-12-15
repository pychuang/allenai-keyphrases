#!/usr/bin/python

import csv
import sys

import mysql_util

hash2doi = {}

with open('keymap') as csvfile:
    csvreader = csv.reader(csvfile, delimiter='\t')

    for row in csvreader:
        (id, doi) = row
        hash2doi[id] = doi

doi2keyphrases = {}

with open('keyphrases') as csvfile:
    csvreader = csv.reader(csvfile, delimiter='\t')

    for row in csvreader:
        id = row[0]
        keyphrases = row[1:]

        if id not in hash2doi:
            continue
        doi = hash2doi[id]
        doi2keyphrases[doi] = keyphrases

db, cursor = mysql_util.init_db()

total = len(doi2keyphrases)
try:
    if not mysql_util.does_table_exist(db, cursor, 'paper_keywords_allenai'):
        cursor.execute('CREATE TABLE paper_keywords_allenai ('
            'id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, '
            'doi varchar(100), '
            'keyphrase varchar(255), '
            'INDEX (doi))')

    count = 1
    for doi, keyphrases in doi2keyphrases.iteritems():
        print "\r%d / %d: %s: %s" % (count, total, doi, str(keyphrases)),
        sys.stdout.flush()

        try:
            cursor.execute("""DELETE FROM paper_keywords_allenai WHERE doi=%s""", (doi,))

            for keyphrase in keyphrases:
                cursor.execute("""INSERT INTO paper_keywords_allenai (doi, keyphrase) VALUES (%s, %s)""", (doi, keyphrase))
            db.commit()
        except Exception as e:
            print e
            db.rollback()
        count += 1
finally:
    mysql_util.close_db(db, cursor)
