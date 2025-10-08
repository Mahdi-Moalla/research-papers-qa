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

from langchain_community.document_loaders import PyPDFLoader

from config import CONFIG

import IPython
from IPython.display import display, Markdown

DOWNLOAD_BLOCK_SIZE = 1024  # 1 KB chunks

non_processed_pdfs_sql="""
SELECT *  FROM papers WHERE extract_done=False;
"""

create_table_sql="""
CREATE TABLE IF NOT EXISTS papers_raw_md (
    id INTEGER PRIMARY KEY,
    raw_md BLOB NOT NULL,
    FOREIGN KEY (id) REFERENCES papers (id)
);
"""


def main():

    with duckdb.connect(CONFIG.DB_FILE)  as  con:
        con.execute(create_table_sql)
        nonprocessed_pdfs=con.execute(non_processed_pdfs_sql).df()
        print(nonprocessed_pdfs)


    for i in tqdm(range(len(nonprocessed_pdfs)), position=0, leave=True):
        pdf_record=nonprocessed_pdfs.iloc[i]
        try:
            doc = PyPDFLoader(pdf_record.paper_link,
                   mode='single',
                   extraction_mode='plain').load()

            data=doc[0].page_content
            with duckdb.connect(CONFIG.DB_FILE)  as con:
                con.execute("INSERT OR REPLACE INTO papers_raw_md VALUES (?, ?)",
                           [pdf_record.id.item(), data.encode("utf-8")])
                con.execute(f"UPDATE papers SET extract_done=True WHERE id={pdf_record.id.item()}")

        except:
            print(f"parsing of {pdf_record} failed")


if __name__=='__main__':
    fire.Fire(main)