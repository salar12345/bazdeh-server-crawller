from ncl.utils.common.singleton import Singleton

from bzgi.feed.dal.esdao.car_crawler_esdao import CarCrawlerEsDao


class CarCrawlerDal(metaclass=Singleton):
    def __init__(self):
        self.esdao = CarCrawlerEsDao()

    def create_post(self, doc_id: str, doc: dict):
        self.esdao.create_post(doc_id=doc_id, doc=doc)

    def update_post(self, doc_id: str, doc: dict):
        self.esdao.update_post(doc_id=doc_id, doc=doc)

    def change_persist(self):
        self.esdao.change_persist()

    def exists_car(self, doc_id: str):
        self.esdao.exist_car(doc_id=doc_id)

    def search_post(self, query: dict):
        return self.esdao.search_post(query)


