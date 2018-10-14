import time
import sqlite3
from sys import stdout

def read(dirname):
    with open(dirname, 'r') as input:
        for num, line in enumerate(input, 1):
            yield (num, line)


if __name__ == '__main__':
    wiki_text_dump_path = '../data/ptwiki-articles-cleaned.txt'
    start_time = time.time()
    jobs_concluded = 0
    commit_every = 10000
    with sqlite3.connect('ptwiki.db') as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE sentences (id int PRIMARY KEY, text text, palavras text)''')
        for (num, line) in read(wiki_text_dump_path):
            jobs_concluded += 1
            c.execute("INSERT INTO sentences VALUES ({0},\"{1}\",null)".format(
                num,
                line
            ))
            if (jobs_concluded % commit_every == 0):
                conn.commit()
            elapsed_time = time.time() - start_time
            sentences_processed = jobs_concluded * 1
            sentences_per_second = sentences_processed/elapsed_time
            stdout.write('Processing ...')
            stdout.write('%8d/s, %10d\r' %
                         (sentences_per_second, sentences_processed))
            stdout.flush()
    elapsed_time = time.time() - start_time
    print('Time elapsed: {0}'.format(elapsed_time))


# if __name__ == '__main__':
#     with sqlite3.connect('ptwiki.db') as conn:
#         c = conn.cursor()
#         for row in c.execute('SELECT id FROM sentences where palavras IS NULL'):
#             print(row)

# # u = (parsed_text, id)
# # c.execute('UPDATE sentences SET palavras = ? WHERE id = ?')
# # c.commit()

# CREATE UNIQUE INDEX t1b ON t1(b); 