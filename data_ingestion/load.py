"""
"""

import os
import sys

sys.path.append(os.getcwd())

import re

import fire
from tqdm import tqdm
import requests
import pandas as pd
import duckdb


from config import CONFIG

import IPython

non_processed_chunks_sql="""
SELECT *  
FROM papers_chunks
LEFT JOIN papers ON (papers_chunks.paper_id=papers.id) 
WHERE papers.transform_done=TRUE AND papers.load_done=FALSE;
"""

def main():

    with duckdb.connect(CONFIG.DB_FILE)  as  con:
        nonprocessed_chunks=con.execute(non_processed_chunks_sql).df()
        print(nonprocessed_chunks)

    print(nonprocessed_chunks)

    IPython.embed(colors='Linux')


if __name__=='__main__':
    fire.Fire(main)