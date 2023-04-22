
from ncl.dal.rddao.commons_base_rddao import CommonsBaseRDDao, RDKey
import json

from ncl.utils.common.singleton import Singleton


class LoanCache(CommonsBaseRDDao, metaclass=Singleton):


    LOAN_KEY = RDKey("loan:{loan_id}", ttl=60*60*24)

    def __init__(self) -> None:
        super().__init__()

    def do_cache(self, loan: dict = {}):
        key = self.LOAN_KEY.key.format(loan_id=loan.get('id'))
        value = json.dumps(loan)
        self.client.set(name=key, value=value)
        self.client.expire(name=key, time=60*60*24)

    def get_from_cache(self, loan_id: str = None) -> list:
        loan_info = self.client.get(name=loan_id)
        return loan_info

    def get_single_loan_form_cache(self, loan_id):
        key = self.LOAN_KEY.key.format(loan_id=loan_id)
        loan_info = self.client.get(name=key)
        return loan_info

    def get_all_loan_info(self):
        keys = self.client.keys(pattern="loan*")
        values = {}
        for key in keys:
            value = self.get_from_cache(key)
            values[key] = value
        return values

