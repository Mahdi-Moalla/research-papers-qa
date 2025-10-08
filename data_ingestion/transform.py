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

from langchain_text_splitters import RecursiveCharacterTextSplitter


from config import CONFIG

import IPython
from IPython.display import display, Markdown

non_processed_raws_sql="""
SELECT * FROM papers_raw WHERE is_processed=False
"""

create_table_sql="""
CREATE SEQUENCE IF NOT EXISTS papers_chunks_ids START 1;
CREATE TABLE IF NOT EXISTS papers_chunks (
    id INTEGER DEFAULT nextval('papers_chunks_ids') PRIMARY KEY,
    paper_id INTEGER,
    chunk VARCHAR NOT NULL,
    is_processed BOOL DEFAULT FALSE,
    FOREIGN KEY (paper_id) REFERENCES papers (id)
);
"""



def main():

    with duckdb.connect(CONFIG.DB_FILE)  as  con:
        con.execute(create_table_sql)
        non_processed_raws=con.execute(non_processed_raws_sql).df()
        print(non_processed_raws)

    text_splitter = RecursiveCharacterTextSplitter(
        # Set a really small chunk size, just to show.
        separators=["\n\n", "\n", "."],
        chunk_size=1000,
        chunk_overlap=300,
        length_function=len,
        is_separator_regex=True,
    )

    for i in tqdm(range( len(non_processed_raws) )):
        pdf_record=non_processed_raws.iloc[i]

        paper_id=pdf_record.id.item()

        text=pdf_record.raw_text#.decode("utf-8")
        l=text.lower().find("references")
        if l!=-1:
            text=text[:l]
        
        text_chunks = [t.page_content for t in text_splitter.create_documents([text])]

        with duckdb.connect(CONFIG.DB_FILE)  as con:
            con.executemany("INSERT OR REPLACE INTO papers_chunks  (paper_id,chunk) VALUES (?, ?)",
                           [[paper_id,t] for  t in text_chunks])
            con.execute(f"UPDATE papers_raw SET is_processed=True WHERE id={pdf_record.id.item()}")


if __name__=='__main__':
    fire.Fire(main)