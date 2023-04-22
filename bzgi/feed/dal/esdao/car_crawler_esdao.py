from ncl.dal.esdao.bulk_esdao import AbstractBulkESDao as BaseBulkESDao

from bzgi.config import BZSCConfig


class CarCrawlerEsDao(BaseBulkESDao):
    def __init__(self):
        super().__init__()
        BaseBulkESDao.__init__(self)
        self.index_name = BZSCConfig.CAR_INDEX_NAME

    def create_post(self, doc_id: str, doc: dict):
        self.bulk_create(doc_id=doc_id, doc=doc, index_name=self.index_name)

    def update_post(self, doc_id: str, doc: dict):
        self.bulk_update(doc_id=doc_id, doc=doc, index_name=self.index_name)

    def change_persist(self):
        self.flush()

    def exist_car(self, doc_id: str):
        self.exists(doc_id=doc_id, index_name=self.index_name)

    def search_post(self, query: dict):
        post = self.search(index_name=self.index_name, query=query)
        result = post.get("hits").get("hits")
        return result

