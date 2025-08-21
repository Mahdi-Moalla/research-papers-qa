"""
MongoDB interface
"""
from typing import *
from pymongo import MongoClient
from bson.objectid import ObjectId
from pydantic import validate_call

class MongoDB:
    """
    MongoDB interface
    """
    DATABASE_NAME: str="research-papers-qa-db"
    
    @validate_call
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

    #@validate_call
    def __init__(self,
                 mongo_db: MongoDB):
        
        assert isinstance(mongo_db, MongoDB)
        
        self.db = mongo_db
        self.collection = mongo_db.db[self.COLLECTION_NAME]


@validate_call
def validate_tag(tag: str):
    """
    tag validator
    """
    tag=tag.strip().lower()
    assert all(c.isalpha() or c.isspace() \
               for c in tag)
    return tag

class PapersCollection(MongoCollection):
    """
    handling papers collection
    """
    COLLECTION_NAME='papers'

    @validate_call
    def insert_paper(self,
                     source:  str,
                     source_id: str,
                     download_link: str,
                     tag: str):
        """
        insert single paper
        """
        tag=validate_tag(tag)
        self.collection.insert_one({
            "_id": f"source:{source},id:{source_id}",
            "source": source,
            "source_id": source_id,
            "download_link": download_link,
            "processed": False,
            "tag": tag
        })

    @validate_call
    def insert_papers(self,
                      sources:  List[str],
                      source_ids: List[str],
                      download_links: List[str],
                      tags: List[str]):
        """
        insert multiple papers
        """
        assert len(sources)==len(source_ids) and \
            len(sources)==len(download_links) and \
            len(sources)==len(tags)
        tags=[validate_tag(tag) for tag in tags]
        
        self.collection.insert_many([{
            "_id": f"source:{source},id:{source_id}",
            "source": source,
            "source_id": source_id,
            "download_link": download_link,
            "processed": False,
            "tag": tag
        } for source, source_id, download_link, tag in \
            zip(sources, source_ids, download_links, tags)])

    @validate_call
    def paper_set_processed(self,
                            paper_id: str):
        """
        set paper as processed
        """
        self.collection.update_one(
            {"_id":paper_id},
            {"$set":{"processed":True}}
        )

    @validate_call
    def get_non_processed_papers(self):
        """
        get non processed papers
        """
        return list(
            self.collection.find({"processed":False}))


class PagesCollection(MongoCollection):
    """
    Handling pages collection
    """
    COLLECTION_NAME='pages'

    @validate_call
    def add_paper_pages(self,
                        paper_id: str,
                        paper_pages: List[str],
                        tag: str):
        """
        add pages
        """
        tag=validate_tag(tag)
        self.collection.insert_many([{
            "paper_id": paper_id,
            "page_idx": page_i,
            "page_text": page,
            "tag": tag,
            "processed": False
        } for page_i, page in enumerate(paper_pages)])

    @validate_call
    def page_set_processed(self,
                           page_id: str):
        """
        set page as processed
        """
        self.collection.update_one(
            {"_id":ObjectId(page_id)},
            {"$set":{"processed":True}}
        )

    @validate_call
    def get_non_processed_pages(self):
        """
        get non processed pages
        """
        return list(
            self.collection.find({"processed":False}))


class PragraphsCollection(MongoCollection):
    """
    Handling paragraphs collection
    """
    COLLECTION_NAME='paragraphs'

    @validate_call
    def add_paragraphs(self,
                       papers_ids: List[str],
                       pages_idxs: List[int],
                       paragrahs: List[str],
                       tags: List[str]):
        """
        add paragraphs
        """
        assert len(papers_ids)==len(pages_idxs) and \
            len(papers_ids)==len(paragrahs) and \
            len(papers_ids)==len(tags)
        
        tags=[validate_tag(tag) for tag in tags]
        
        self.collection.insert_many([{
            "paper_id": paper_id,
            "page_idx": page_idx,
            "paragraph_text": paragraph,
            "tag": tag,
            "processed": False
        } for paper_id, page_idx, paragraph, tag in \
            zip(papers_ids, pages_idxs, paragrahs, tags)])

    @validate_call
    def paragraph_set_processed(self,
                                paragraph_id: str):
        """
        set paragraph as processed
        """
        self.collection.update_one(
            {"_id":ObjectId(paragraph_id)},
            {"$set":{"processed":True}}
        )

    @validate_call
    def get_non_processed_paragraphs(self):
        """
        get non processed paragraphs
        """
        return list(
            self.collection.find({"processed":False}))

if __name__=='__main__':

    from pprint import pprint

    URI = "mongodb://mongo_user:mongo_pass@localhost:27017/"
    mongo_db =MongoDB(URI)
    mongo_db.drop_database()
    mongo_db =MongoDB(URI)

    papers_collection = PapersCollection(mongo_db)
    papers_collection.insert_paper("arxiv",
                                   "1234",
                                   "http://xxxxxxx",
                                   "tag a")
    
    papers_collection.insert_papers(["arxiv","arxiv"],
                                    ["1111111","222222"],
                                    ["http://xxxxxxx","http://yyyyyyy"],
                                    ("tag A","tag B"))
    pprint(papers_collection.get_non_processed_papers(),
           indent=2)
    
    pages_collection = PagesCollection(mongo_db)

    pages_collection.add_paper_pages('source:arxiv,id:1234',
                                     ["aaaaaaa","bbbbbbbb"],
                                     "tag a")
