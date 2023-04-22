from ncl.utils.common.singleton import Singleton

from bzgi.serve.dal.esdao.loan_esdao import LoanEsDao
from bzgi.serve.dal.rddao.loan_rddao import LoanCache


class LoanDal(metaclass=Singleton):
    def __init__(self):
        self.esdao = LoanEsDao()
        self.rddao = LoanCache()


    def search_post(self, query: dict):
        return self.esdao.search_post(query)

    def do_cache(self, loan_single: dict = None):
        self.rddao.do_cache(loan_single)

    def get_all_loan_info(self):
        return self.rddao.get_all_loan_info()

    def get_single_loan_form_cache(self, loan_id):
        return self.rddao.get_single_loan_form_cache(loan_id=loan_id)

    def get_related_loan_dal(self, query):
        return self.esdao.get_related_loan_dao(query)
