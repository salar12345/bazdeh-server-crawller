from ncl.utils.common.singleton import Singleton

from bzgi.feed.dal.esdao.loan_crawler_esdao import LoanCrawlerEsDao


class LoanCrawlerDal(metaclass=Singleton):
    def __init__(self):
        self.esdao = LoanCrawlerEsDao()

    def create_post(self, doc_id: str, doc: dict):
        self.esdao.create_post(doc_id=doc_id, doc=doc)

    def update_post(self, doc_id: str, doc: dict):
        self.esdao.update_post(doc_id=doc_id, doc=doc)

    def change_persist(self):
        self.esdao.change_persist()

    def search_post(self, query: dict):
        return self.esdao.search_post(query)

