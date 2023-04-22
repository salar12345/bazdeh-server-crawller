from ncl.dal.esdao.bulk_esdao import AbstractBulkESDao as BaseBulkESDao
from ncl.utils.config.configuration import Configuration

from bzgi.config import BZSCConfig
from bzgi.model.vo.elastic_search_vo import ElasticSearchCommonsVO


class CarEsDao(BaseBulkESDao):
    def __init__(self):
        super().__init__()
        BaseBulkESDao.__init__(self)
        self.index_name = BZSCConfig.CAR_INDEX_NAME


    def search_post(self, query: dict):
        post = self.search(index_name=self.index_name, query=query)
        result = post.get(ElasticSearchCommonsVO.HITS).get(ElasticSearchCommonsVO.HITS)
        return result



# if __name__ == '__main__':
#     from bzgi.config import BZSCConfig
#
#     Configuration.configure(BZSCConfig, alternative_env_search_dir=__file__)
#     CarEsDao().search_post(query={})
