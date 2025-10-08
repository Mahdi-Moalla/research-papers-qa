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

non_processed_pdfs_sql="""
SELECT *  
FROM papers
INNER JOIN papers_raw_md USING (id) 
WHERE papers.extract_done=TRUE AND papers.transform_done=FALSE;
"""

create_table_sql="""
CREATE SEQUENCE IF NOT EXISTS papers_chunks_ids START 1;
CREATE TABLE IF NOT EXISTS papers_chunks (
    id INTEGER DEFAULT nextval('papers_chunks_ids') PRIMARY KEY,
    paper_id INTEGER,
    chunk VARCHAR NOT NULL,
    FOREIGN KEY (paper_id) REFERENCES papers (id)
);
"""



def main():

    with duckdb.connect(CONFIG.DB_FILE)  as  con:
        con.execute(create_table_sql)
        nonprocessed_pdfs=con.execute(non_processed_pdfs_sql).df()
        print(nonprocessed_pdfs)

    text_splitter = RecursiveCharacterTextSplitter(
        # Set a really small chunk size, just to show.
        separators=["\n\n", "\n", "."],
        chunk_size=1000,
        chunk_overlap=300,
        length_function=len,
        is_separator_regex=True,
    )

    for i in tqdm(range( len(nonprocessed_pdfs) )):
        pdf_record=nonprocessed_pdfs.iloc[i]

        paper_id=pdf_record.id.item()

        text=pdf_record.raw_md.decode("utf-8")
        l=text.lower().find("references")
        if l!=-1:
            text=text[:l]
        
        text_chunks = [t.page_content for t in text_splitter.create_documents([text])]

        with duckdb.connect(CONFIG.DB_FILE)  as con:
            con.executemany("INSERT OR REPLACE INTO papers_chunks  (paper_id,chunk) VALUES (?, ?)",
                           [[paper_id,t] for  t in text_chunks])
            con.execute(f"UPDATE papers SET transform_done=True WHERE id={pdf_record.id.item()}")


if __name__=='__main__':
    fire.Fire(main)