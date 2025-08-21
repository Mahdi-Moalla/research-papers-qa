"""
MongoDB interface
"""
from typing import *
from pymongo import MongoClient


class MongoDB:
    """
    MongoDB interface
    """
    DATABASE_NAME="research-papers-qa-db"
    def __init__(self,
                 server_uri: str):

        self.client=MongoClient(server_uri)
        self.db=self.client[self.DATABASE_NAME]

    def drop_database(self):
        """
        drop database
        """
        self.client.drop_database(self.DATABASE_NAME)

    #def __del__(self):
    #    self.client.close()


class MongoCollection:
    """
    General collection class
    """
    COLLECTION_NAME=None

    def __init__(self,
                 mongo_db: MongoDB):
        self.db = mongo_db
        self.collection = mongo_db.db[self.COLLECTION_NAME]



class PapersCollection(MongoCollection):
    """
    handling papers collection
    """
    COLLECTION_NAME='papers'

    def insert_paper(self,
                     source:  str,
                     source_id: str,
                     download_link: str,
                     tag: str):
        """
        insert  paper
        """
        self.collection.insert_one({
            "_id": f"source:{source},id:{source_id}",
            "source": source,
            "source_id": source_id,
            "download_link": download_link,
            "processed": False,
            "tag": tag
        })

    def insert_papers(self,
                      sources:  List[str],
                      source_ids: List[str],
                      download_links: List[str],
                      tags: List[str]):
        """
        insert  paper
        """
        self.collection.insert_many([{
            "_id": f"source:{source},id:{source_id}",
            "source": source,
            "source_id": source_id,
            "download_link": download_link,
            "processed": False,
            "tag": tag
        } for source, source_id, download_link, tag in \
            zip(sources, source_ids, download_links, tags)])

    def paper_set_processed(self,
                            source: str,
                            source_id: str):
        """
        set paper as processed
        """
        self.collection.update_one(
            {"_id":f"source:{source},id:{source_id}"},
            {"$set":{"processed":True}}
        )
    def get_non_processed_papers(self):
        """
        get non processed papers
        """
        return list(
            self.collection.find({"processed":False}))


class PagesCollection:
    """
    Handling pages collection
    """
    COLLECTION_NAME='pages'

    def add_pages(self,
                 paper_id: str,
                 paper_text: List[str],
                 tag: str):
        """
        add pages
        """


if __name__=='__main__':

    URI = "mongodb://mongo_user:mongo_pass@localhost:27017/"
    mongo_db =MongoDB(URI)
    mongo_db.drop_database()
    mongo_db =MongoDB(URI)

    papers_collection = PapersCollection(mongo_db)
    papers_collection.insert_paper("arxiv",
                                   "1234:1234",
                                   "http://xxxxxxx",
                                   "image classification")
