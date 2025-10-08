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

from fastembed import TextEmbedding
from qdrant_client import QdrantClient, models

from config import CONFIG

import IPython

non_processed_chunks_sql="""
SELECT * FROM papers_chunks WHERE is_processed=False
"""


def list_embed_models():
    supported_models = (
        pd.DataFrame(TextEmbedding.list_supported_models())
        .sort_values("dim")
        .drop(columns=["sources", "model_file", "additional_files"])
        .reset_index(drop=True)
    )
    sep="\n#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#@#\n"
    [print(supported_models.loc[i],sep) for  i in range(len(supported_models))]

def main():
    with duckdb.connect(CONFIG.DB_FILE)  as  con:
        nonprocessed_chunks=con.execute(non_processed_chunks_sql).df()
        print(nonprocessed_chunks)

    print(nonprocessed_chunks)

    embedding_model = TextEmbedding(model_name=CONFIG.EMBEDDING_MODEL)
    embedding_dim = len(list(embedding_model.embed(["hello"]))[0])

    client = QdrantClient(CONFIG.QDRANT_SERVER_URL)

    if not client.collection_exists(collection_name=CONFIG.QDRANT_COLLECTION_NAME):
        client.create_collection(
            collection_name=CONFIG.QDRANT_COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=embedding_dim,  # Dimensionality of the vectors
                distance=models.Distance.COSINE  # Distance metric for similarity search
            )
        )



    for  i in tqdm(range(len(nonprocessed_chunks))):
        chunk_record=nonprocessed_chunks.iloc[i]

        point = models.PointStruct(
            id=chunk_record.id.item(),
            vector=models.Document(text=chunk_record.chunk, 
                                   model=CONFIG.EMBEDDING_MODEL),
            payload={} #save all needed metadata fields
        )

        
        client.upsert(
            collection_name=CONFIG.QDRANT_COLLECTION_NAME,
            points=[point]
        )

        with duckdb.connect(CONFIG.DB_FILE) as con:
            con.execute(f"UPDATE  papers_chunks SET is_processed=TRUE WHERE id={chunk_record.id.item()}")


    IPython.embed(colors='Linux')


if __name__=='__main__':
    fire.Fire()