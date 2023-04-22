from ncl.logic.commons_base_logic import CommonsBaseLogic

import json

from ncl.utils.common.singleton import Singleton

from bzgi.model.vo.elastic_search_vo import ElasticSearchCommonsVO
from bzgi.model.vo.elastic_vo import ElasticVO

from bzgi.model.vo.loan_vo import LoanVO
from bzgi.serve.dal.loan_dal import LoanDal


class LoanLogic(CommonsBaseLogic, metaclass=Singleton):

    def __init__(self):
        super().__init__()
        self.dal = LoanDal()

    def get_loan_list(self):
        result = []
        loan_cache = self.get_loans_from_cache()
        if not loan_cache:
            self.do_cache_all_loans()
            loan_cache = self.get_loans_from_cache()

        for key in loan_cache:
            loan = loan_cache[key]
            loan_dic = json.loads(loan)
            # loan_dic['id'] = key
            result.append(self.get_important_loan_dic(loan_dic))

        return result

    def get_loan_by_group_logic(self):
        loan_by_group = self.dal.search_post(query={})

        return loan_by_group

    def get_loan_single_by_id_logic(self, loan_id: str = None):
        result = self.dal.get_single_loan_form_cache(loan_id)
        # dict_result = self.create_single_loan_model(result)
        dict_result = json.loads(result)
        return dict_result

    def get_important_loan_dic(self, loan_model: dict = {}):
        loan_dict = {}
        loan_dict[LoanVO.IMAGE] = loan_model[LoanVO.IMAGE]
        loan_dict[LoanVO.NAME] = loan_model[LoanVO.NAME]
        if loan_model[LoanVO.INTEREST_REVENUE]:
            loan_dict[LoanVO.INTEREST_REVENUE] = loan_model[LoanVO.INTEREST_REVENUE].split("\n")[0]
        else:
            loan_dict[LoanVO.INTEREST_REVENUE] = loan_model[LoanVO.INTEREST_REVENUE]
        if loan_model[LoanVO.MAXIMUM_PAYMENT_TIME]:
            loan_dict[LoanVO.MAXIMUM_PAYMENT_TIME] = loan_model[LoanVO.MAXIMUM_PAYMENT_TIME].split("\n")[0]
        else:
            loan_dict[LoanVO.MAXIMUM_PAYMENT_TIME] = loan_model[LoanVO.MAXIMUM_PAYMENT_TIME]
        if loan_model[LoanVO.TYPE_OF_GUARANTEE]:
            loan_dict[LoanVO.TYPE_OF_GUARANTEE] = loan_model[LoanVO.TYPE_OF_GUARANTEE].split("\n")[0]
        else:
            loan_dict[LoanVO.TYPE_OF_GUARANTEE] = loan_model[LoanVO.TYPE_OF_GUARANTEE]
        if loan_model[LoanVO.MAX_LOAN]:
            loan_dict[LoanVO.MAX_LOAN] = loan_model[LoanVO.MAX_LOAN].split("\n")[0]
        else:
            loan_dict[LoanVO.MAX_LOAN] = loan_model[LoanVO.MAX_LOAN]

        loan_dict[LoanVO.LOAN_TYPE] = loan_model[LoanVO.LOAN_TYPE]
        loan_dict[LoanVO.ID] = loan_model[LoanVO.ID]
        loan_dict[LoanVO.LOAN_TYPE_ID] = loan_model[LoanVO.LOAN_TYPE_ID]

        return loan_dict

    def create_single_loan_model(self, loan_model):
        loan_dict = {}
        # loan_model = json.loads(loan_model)
        loan_id = loan_model.get("_id")
        loan_model = loan_model.get(LoanVO.SOURCE)
        # loan_id = self._create_loan_id(str(loan_model.get(LoanVO.SINGLE_URL)))
        loan_dict[LoanVO.ID] = loan_id
        loan_dict[LoanVO.IMAGE] = loan_model.get(LoanVO.IMAGE)
        loan_dict[LoanVO.NAME] = loan_model.get(LoanVO.NAME)
        loan_dict[LoanVO.INTEREST_REVENUE] = loan_model.get(LoanVO.INTEREST_REVENUE)
        loan_dict[LoanVO.DEPOSIT_REQUIRED] = loan_model.get(LoanVO.DEPOSIT_REQUIRED)
        loan_dict[LoanVO.MAXIMUM_PAYMENT_TIME] = loan_model.get(LoanVO.MAXIMUM_PAYMENT_TIME)
        loan_dict[LoanVO.TYPE_OF_GUARANTEE] = loan_model.get(LoanVO.TYPE_OF_GUARANTEE)
        loan_dict[LoanVO.MAX_LOAN] = loan_model.get(LoanVO.MAX_LOAN)
        loan_dict[LoanVO.LOAN_INSTALLMENT] = loan_model.get(LoanVO.LOAN_INSTALLMENT)
        loan_dict[LoanVO.OPPORTUNITY_COST_BLOCKED] = loan_model.get(LoanVO.OPPORTUNITY_COST_BLOCKED)
        loan_dict[LoanVO.LOAN_TYPE] = loan_model.get(LoanVO.LOAN_TYPE)
        loan_dict[LoanVO.COMPANY] = loan_model.get(LoanVO.COMPANY)
        loan_dict[LoanVO.LOAN_NAME] = loan_model.get(LoanVO.LOAN_NAME)
        loan_dict[LoanVO.LOAN_MIN] = loan_model.get(LoanVO.LOAN_MIN)
        loan_dict[LoanVO.BLOCKED_DEPOSIT] = loan_model.get(LoanVO.BLOCKED_DEPOSIT)
        loan_dict[LoanVO.DEPOSIT_TIME_LIMIT] = loan_model.get(LoanVO.DEPOSIT_TIME_LIMIT)
        loan_dict[LoanVO.DEPOSIT_ACCOUNT] = loan_model.get(LoanVO.DEPOSIT_ACCOUNT)
        loan_dict[LoanVO.DEPOSIT_VALUE] = loan_model.get(LoanVO.DEPOSIT_VALUE)
        loan_dict[LoanVO.DEPOSIT_PROFIT] = loan_model.get(LoanVO.DEPOSIT_PROFIT)
        loan_dict[LoanVO.DEPOSIT_SLEEP_DURATION] = loan_model.get(LoanVO.DEPOSIT_SLEEP_DURATION)
        loan_dict[LoanVO.RATIO_LOAN_TO_DEPOSIT] = loan_model.get(LoanVO.RATIO_LOAN_TO_DEPOSIT)
        loan_dict[LoanVO.TOTAL_PROFIT] = loan_model.get(LoanVO.TOTAL_PROFIT)
        loan_dict[LoanVO.TOTAL_LOAN_PROFIT] = loan_model.get(LoanVO.TOTAL_LOAN_PROFIT)
        loan_dict[LoanVO.SEPARATE_DEPOSIT] = loan_model.get(LoanVO.SEPARATE_DEPOSIT)
        loan_dict[LoanVO.OTHER_COSTS] = loan_model.get(LoanVO.OTHER_COSTS)
        loan_dict[LoanVO.CONDITIONS] = loan_model.get(LoanVO.CONDITIONS)
        loan_dict[LoanVO.BLOCKED_DEPOSIT_BOOLIAN] = loan_model.get(LoanVO.BLOCKED_DEPOSIT_BOOLIAN)
        loan_dict[LoanVO.REQUIRES_DEPOSIT_BOOLIAN] = loan_model.get(LoanVO.REQUIRES_DEPOSIT_BOOLIAN)
        loan_dict[LoanVO.LOAN_TYPE_ID] = loan_model.get(LoanVO.LOAN_TYPE_ID)
        loan_dict[LoanVO.SINGLE_URL] = loan_model.get(LoanVO.SINGLE_URL)
        loan_dict[LoanVO.RADE_LAST_UPDATE_DATETIME] = loan_model.get(LoanVO.RADE_LAST_UPDATE_DATETIME)
        loan_dict[LoanVO.PROFIT_INTEGER] = loan_model.get(LoanVO.PROFIT_INTEGER)
        loan_dict[LoanVO.MAX_LOAN_INTEGER] = loan_model.get(LoanVO.MAX_LOAN_INTEGER)
        loan_dict[LoanVO.MAXIMUM_PAYMENT_TIME_INTEGER] = loan_model.get(LoanVO.MAXIMUM_PAYMENT_TIME_INTEGER)
        loan_dict[LoanVO.IS_ACTIVE] = loan_model.get(LoanVO.IS_ACTIVE)
        loan_dict[LoanVO.LAST_UPDATE_DATETIME] = loan_model.get(LoanVO.LAST_UPDATE_DATETIME)

        return loan_dict

    def do_cache_loan_logic(self, loan_single: dict = None):

        self.dal.do_cache(loan_single)

    def get_loans_from_cache(self):
        return self.dal.get_all_loan_info()

    @staticmethod
    def _create_all_loan_query():
        query = {
            "size": 1000
        }
        return query

    def do_cache_all_loans(self):
        query = self._create_all_loan_query()
        dao_result = self.dal.search_post(query=query)
        for loan in dao_result:
            single_loan = self.create_single_loan_model(loan)
            self.do_cache_loan_logic(single_loan)

    def get_related_loan(self, loan_amount: float = 0, num_of_installment: int = 0, profit: float = 0):
        # self.get_loan_list()
        loan_amount = loan_amount / 1000000
        loan_amount_min = loan_amount - 50
        loan_amount_max = loan_amount + 50
        num_of_installment_min = num_of_installment - 5
        num_of_installment_max = num_of_installment + 5
        profit_min = profit - 4
        profit_max = profit + 4
        query = self._create_related_loan_query(loan_amount_min=loan_amount_min, loan_amount_max=loan_amount_max,
                                                num_of_installment_min=num_of_installment_min,
                                                num_of_installment_max=num_of_installment_max, profit_min=profit_min,
                                                profit_max=profit_max)
        related_loans_list = []
        related_loans = self.dal.get_related_loan_dal(query)
        for related_loan in related_loans:
            related_loans_list.append(self.create_related_loan_model(related_loan))
        return related_loans_list

    def _create_related_loan_query(self, loan_amount_min, loan_amount_max, num_of_installment_min,
                                   num_of_installment_max, profit_min, profit_max):
        query = {
            ElasticVO.QUERY: {
                ElasticVO.BOOL: {
                    ElasticVO.MUST: [
                        {
                            ElasticVO.RANGE: {
                                LoanVO.MAX_LOAN_INTEGER: {
                                    ElasticVO.GTE: loan_amount_min,
                                    ElasticVO.LTE: loan_amount_max
                                }
                            }
                        },
                        {
                            ElasticVO.RANGE: {
                                LoanVO.PROFIT_INTEGER: {
                                    ElasticVO.GTE: profit_min,
                                    ElasticVO.LTE: profit_max
                                }
                            }
                        },
                        {
                            ElasticVO.RANGE: {
                                LoanVO.MAXIMUM_PAYMENT_TIME_INTEGER: {
                                    ElasticVO.GTE: num_of_installment_min,
                                    ElasticVO.LTE: num_of_installment_max
                                }
                            }
                        }
                    ]
                }

            }
        }
        return query

    def _create_loan_id(self, share_url):
        pass

    def create_related_loan_model(self, loan_model):
        loan_id = loan_model.get(ElasticSearchCommonsVO.ID)
        loan_model = loan_model.get(ElasticSearchCommonsVO.SOURCE)
        loan_dict = {}

        loan_dict[LoanVO.ID] = loan_id
        loan_dict[LoanVO.IMAGE] = loan_model.get(LoanVO.IMAGE)
        loan_dict[LoanVO.NAME] = loan_model.get(LoanVO.NAME)
        loan_dict[LoanVO.LOAN_TYPE] = loan_model.get(LoanVO.LOAN_TYPE)
        if loan_model.get(LoanVO.INTEREST_REVENUE) is None:
            loan_dict[LoanVO.INTEREST_REVENUE] = None
        else:
            loan_dict[LoanVO.INTEREST_REVENUE] = loan_model.get(LoanVO.INTEREST_REVENUE).split("\n")[0]

        if loan_model.get(LoanVO.TYPE_OF_GUARANTEE) is None:
            loan_dict[LoanVO.TYPE_OF_GUARANTEE] = None
        else:
            loan_dict[LoanVO.TYPE_OF_GUARANTEE] = loan_model.get(LoanVO.TYPE_OF_GUARANTEE).split("\n")[0]

        if loan_model.get(LoanVO.MAX_LOAN) is None:
            loan_dict[LoanVO.MAX_LOAN] = None
        else:
            loan_dict[LoanVO.MAX_LOAN] = loan_model.get(LoanVO.MAX_LOAN).split("\n")[0]

        if loan_model.get(LoanVO.MAXIMUM_PAYMENT_TIME) is None:
            loan_dict[LoanVO.MAXIMUM_PAYMENT_TIME] = None
        else:
            loan_dict[LoanVO.MAXIMUM_PAYMENT_TIME] = loan_model.get(LoanVO.MAXIMUM_PAYMENT_TIME).split("\n")[0]

        return loan_dict

