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

from langchain_core.document_loaders import Blob
# from langchain_pymupdf4llm import PyMuPDF4LLMParser
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


    # parser = PyMuPDF4LLMParser(mode="single")

    for i in tqdm(range(len(nonprocessed_pdfs)), position=0, leave=True):
        pdf_record=nonprocessed_pdfs.iloc[i]
        # bytes_chunks=[]
        # try:
        #     response=requests.get(pdf_record.paper_link,
        #                           stream=True)
        #     response.raise_for_status()
        #     total_size = int(response.headers.get('content-length', 0))
        #     with tqdm(total=total_size,
        #               unit='B',
        #               unit_scale=True,
        #               desc=f'{i}',
        #               position=1,
        #               leave=False) as pbar:
        #         for chunk in response.iter_content(chunk_size=DOWNLOAD_BLOCK_SIZE):
        #             bytes_chunks.append(chunk)
        #             pbar.update(len(chunk))
        
        # except:
        #     print(f"Download of {pdf_record} failed")
        #     continue

        # pdf_blob=Blob.from_data(b"".join(bytes_chunks))
        
        try:
            # doc = parser.parse(pdf_blob)
            doc = PyPDFLoader(pdf_record.paper_link,
                   mode='single',
                   extraction_mode='plain').load()
            # with open('./pdf.md','w') as f:
            #     f.write(doc[0].page_content)
            data=doc[0].page_content
            with duckdb.connect(CONFIG.DB_FILE)  as con:
                con.execute("INSERT OR REPLACE INTO papers_raw_md VALUES (?, ?)",
                           [pdf_record.id.item(), data.encode("utf-8")])
                con.execute(f"UPDATE papers SET extract_done=True WHERE id={pdf_record.id.item()}")

        except:
            print(f"parsing of {pdf_record} failed")
        
        #docs = list(doc_generator)
        #IPython.embed(colors='Linux')


if __name__=='__main__':
    fire.Fire(main)