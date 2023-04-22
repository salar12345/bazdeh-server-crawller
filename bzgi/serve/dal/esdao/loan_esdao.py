from ncl.dal.esdao.bulk_esdao import AbstractBulkESDao as BaseBulkESDao

from bzgi.config import BZSCConfig
from bzgi.model.vo.elastic_search_vo import ElasticSearchCommonsVO


class LoanEsDao(BaseBulkESDao):
    def __init__(self):
        super().__init__()
        BaseBulkESDao.__init__(self)
        self.index_name = BZSCConfig.LOAN_INDEX_NAME


    def search_post(self, query: dict):
        post = self.search(index_name=self.index_name, query=query)
        result = post.get(ElasticSearchCommonsVO.HITS).get(ElasticSearchCommonsVO.HITS)
        return result


    def get_related_loan_dao(self, query):
        posts = self.search_post(query)
        return posts

    # def get_related_loan_dao(self, loan_amount, num_of_installment, profit):
    #     loan_amount = loan_amount / 1000000
    #     loan_amount_min = loan_amount - 50
    #     loan_amount_max = loan_amount + 50
    #     num_of_installment_min = num_of_installment - 5
    #     num_of_installment_max = num_of_installment + 5
    #     profit_min = profit - 4
    #     profit_max = profit + 4
    #     related_loans = LoanEntity.query.filter(
    #         and_(LoanEntity.max_loan_integer < loan_amount_max, LoanEntity.max_loan_integer > loan_amount_min)).filter(
    #         and_(LoanEntity.maximum_payment_time_integer > num_of_installment_min,
    #              LoanEntity.maximum_payment_time_integer < num_of_installment_max)).filter(
    #         and_(LoanEntity.profit_integer > profit_min, LoanEntity.profit_integer < profit_max)).all()
    #     return related_loans

