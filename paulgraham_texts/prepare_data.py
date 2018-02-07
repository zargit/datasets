"""
Author: Abdullah Ahmad Zarir
Site: zargit.github.io
"""
import sys
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup as bs

# Uncomment the following block to fetch the articles

def extract():
    base_url = "http://www.paulgraham.com"
    paulgraham_articles_page_url = base_url+"/articles.html"

    page = urlopen(paulgraham_articles_page_url)

    dom = bs(page, 'html.parser')

    essays = dom.select('body table table:nth-of-type(2) a')

    links = [link['href'] for link in essays if 'http' not in link['href']]

    for link in links:
        with open('./raw_data/'+link.split('.')[0]+'.txt', 'w') as f:
            page = urlopen(base_url+'/'+link)
            dom = bs(page, "html.parser")
            elem = dom.select_one('body table table:nth-of-type(1) font')
            print('---\n'+elem.get_text()+'\n---')
            f.write(elem.get_text())

def process():
    text_files = [os.path.abspath(os.path.join('./raw_data', name)) for name in os.listdir("./raw_data")]
    with open("./raw_data/inputs.txt", "w+") as f:
        for path in text_files:
            print("Processing {} ...".format(path))
            with open(path, "r") as t:
                text = t.read().split('\n')
                lines = [line for line in text if line.strip()!='']
                text = ' '.join(lines)
                f.write(text)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == "extract":
            extract()
        elif sys.argv[1].lower() == "process":
            process()
        else:
            print("Use the any of the following option: [extract|process].")
    else:
        print("No option provided, use either [extract|process].")

