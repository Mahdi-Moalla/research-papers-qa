"""
"""

import os
import re

import fire
from tqdm import tqdm
import requests

import duckdb


from IPython import embed

EXAMPLE_URL='https://raw.githubusercontent.com/gmalivenko/awesome-computer-vision-models/refs/heads/master/README.md'
DUCKDB_FILE="data.duckdb"


create_table_sql="""
CREATE TABLE IF NOT EXISTS papers_links (
    paper_link    VARCHAR PRIMARY KEY,
    processed     BOOL
);
"""

def process_link(link):
    if 'openaccess.thecvf' in link and '.pdf' in link:
        return link
    elif 'arxiv' in link:
        return link.replace('abs','pdf').replace('.pdf','')
    else:
        return None

def main(url=EXAMPLE_URL,
         duckdb_file=DUCKDB_FILE):
    
    markdown_index = requests.get(url)
    
    lines=markdown_index.text.split('\n')
    
    links=[]

    for line in lines:
        match=re.search(r"\[[^\[\]]*\]\([^\(\)]*\)",line)
        
        if match is None:
            continue
        match=re.search(r"\(http.*\)",match.group(0))
        if match is not None:
            link=match.group(0)[1:-1]
        else:
            continue
        link=process_link(link)
        if isinstance(link, str):
            links.append(link)

    with duckdb.connect(duckdb_file) as con:
        con.execute(create_table_sql)
        con.executemany("INSERT OR IGNORE INTO papers_links VALUES (?, ?)",
                        [[link,False] for link in links] )


    embed(colors='Linux')





if __name__=='__main__':
    fire.Fire(main)