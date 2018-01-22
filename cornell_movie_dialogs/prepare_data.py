"""
Author: Abdullah Ahmad Zarir
Email: abdullahzarir@gmail.com

This script is going to process the cornell movie dialogs for training a
chatbot.

Data will be collected from the following two files inside 'raw_data/cornell
movie-dialogs corpus' -
    1. movie_conversations.txt
    2. movie_lines.txt

The raw data is encoded in ISO-8859-2 format, this will be converted to UTF-8.
"""
import random

def get_conversations(filename):
    """Extract line numbers for each conversation.

    Each line describes one conversation. Split each line with delimiter
    ' +++$+++ ', then convert the last element to a list. Each item in that
    list is the movie line number ordered chronologically based on that
    conversation. This number will be used to reproduce the original script.

    Args:
        filename <str>: Path of the conversation file from the corpus.
    
    Returns:
        A list of conversations containing all the line numbers.
    """
    decode_type = "ISO-8859-2"
    encode_type = "UTF-8"
    delimiter = ' +++$+++ '
    convs = []
    with open(filename, "r") as datafile:
        for line in datafile.read().split('\n'):
            line = line.decode(decode_type).encode(encode_type)
            conv = line.split(delimiter)[-1][1:-1].replace('\'', '').split(', ')
            convs.append(conv)
    return convs

def get_movie_lines(filename):
    """Extract each movie line with id.

    Each line describes one dialog in a conversation. Split each line with
    delimiter ' +++$+++ ', then use first element as line id and last element
    as line content. Use those two to generate a <dict> of movie lines.

    Args:
        filename <str>: Path of the movie lines file from the corpus.

    Returns:
        A dict of movie lines with corresponding id.
    """
    decode_type = "ISO-8859-2"
    encode_type = "UTF-8"
    delimiter = ' +++$+++ '
    m_lines = {}
    with open(filename, "r") as datafile:
        for line in datafile.read().split('\n'):
            line = line.decode(decode_type).encode(encode_type)
            line = line.split(delimiter)
            key = line[0]
            value = line[-1]
            m_lines[key] = value
    return m_lines

def prepare_dataset(convs, lines, testset_size=30000):
    person_one = []
    person_two = []
    for conv in convs:
        if len(conv)%2 != 0:
            conv = conv[:-1]
        for i in range(0, len(conv), 2):
            person_one.append(conv[i])
            person_two.append(conv[i+1])
    
    testids = random.sample(person_one, testset_size)

    p1 = open("dialogs.p1", "w")
    p2 = open("dialogs.p2", "w")
    tp1 = open("test.p1", "w")
    tp2 = open("test.p2", "w")

    for i in range(len(person_one)):
        if person_one[i] in testids:
            tp1.write(lines[person_one[i]]+'\n')
            tp2.write(lines[person_two[i]]+'\n')
        else:
            p1.write(lines[person_one[i]]+'\n')
            p2.write(lines[person_two[i]]+'\n')
    p1.close()
    p2.close()
    tp1.close()
    tp2.close()



if __name__ == '__main__':
    
    # Filepath to the movie_conversations.txt and movie_lines.txt
    conv_fp = "./raw_data/cornell movie-dialogs corpus/movie_conversations.txt"
    lines_fp = "./raw_data/cornell movie-dialogs corpus/movie_lines.txt"

    convs = get_conversations(conv_fp)
    m_lines = get_movie_lines(lines_fp)
    
    prepare_dataset(convs, m_lines)
