from bzscl.proto.bazdeh.media.loan.loan_pb2 import GetLoansListResponse, GetLoansListQuery, GetSingleLoanQuery, \
    GetSingleLoanResponse, GetRelatedListQuery, GetRelatedListResponse
from bzscl.proto.bazdeh.media.loan.loan_pb2_grpc import LoanServeServicer
from sentry_sdk import start_span

from bzgi.serve.logic.loan_logic import LoanLogic

from bzgi.model.vo.loan_vo import LoanVO
from bzgi.model.vo.logger_vo import LoggerVO
from bzgi.utils.utils import Utils


class LoanImp(LoanServeServicer):
    def __init__(self):
        super().__init__()
        self.utils = Utils()
        self.logic = LoanLogic()

    def GetLoansList(self, request: GetLoansListQuery, context) -> GetLoansListResponse:
        try:
            loan_list = self.logic.get_loan_list()
            response = self._create_list_search_response(loan_list)
            return response
        except Exception as e:
            print(e)

    def GetSingleLoan(self, request: GetSingleLoanQuery, context):
        try:
            loan_id: str = request.loan_id
            single_loan = self.logic.get_loan_single_by_id_logic(loan_id=loan_id)

            return self._create_single_search_response(single_loan)



        except Exception as exception:
            print(exception)

    def GetRelatedList(self, request: GetRelatedListQuery, context):
        try:
            loan_amount: float = request.max_loan_integer
            num_of_installment: int = request.maximum_payment_time_integer
            profit: float = request.profit_integer
            related_loan_list = self.logic.get_related_loan(loan_amount, num_of_installment, profit)
            return self._create_related_search_response(related_loan_list)
        except Exception as e:
            print(e)

    @staticmethod
    def _create_single_search_response(loan: dict):
        with start_span(op=LoggerVO.SEARCH_RESPONSE):
            response_object: GetSingleLoanResponse = GetSingleLoanResponse()
            single_loan = response_object.single_loan

            loan_id: str = loan[LoanVO.ID]
            if loan_id:
                single_loan.loan_id = loan_id

            # _source: Dict = loan.get(ElasticSearchCommonsVO.SOURCE)

            name: str = loan[LoanVO.NAME]
            if name:
                single_loan.name = name

            company: str = loan[LoanVO.COMPANY]
            if company:
                single_loan.company = company
            loan_name: str = loan[LoanVO.LOAN_NAME]
            if loan_name:
                single_loan.loan_name = loan_name
            interest_revenue: str = loan[LoanVO.INTEREST_REVENUE]
            if interest_revenue:
                single_loan.interest_revenue = interest_revenue
            max_loan: str = loan[LoanVO.MAX_LOAN]
            if max_loan:
                single_loan.max_loan = max_loan
            loan_min: str = loan[LoanVO.LOAN_MIN]
            if loan_min:
                single_loan.loan_min = loan_min
            type_of_guarantee: str = loan[LoanVO.TYPE_OF_GUARANTEE]
            if type_of_guarantee:
                single_loan.type_of_guarantee = type_of_guarantee
            blocked_deposit: str = loan[LoanVO.BLOCKED_DEPOSIT]
            if blocked_deposit:
                single_loan.blocked_deposit = blocked_deposit
            blocked_deposit_boolian: str = loan[LoanVO.BLOCKED_DEPOSIT_BOOLIAN]
            if blocked_deposit_boolian:
                single_loan.blocked_deposit_boolian = blocked_deposit_boolian
            deposit_time_limit: str = loan[LoanVO.DEPOSIT_TIME_LIMIT]
            if deposit_time_limit:
                single_loan.deposit_time_limit = deposit_time_limit
            deposit_required: str = loan[LoanVO.DEPOSIT_REQUIRED]
            if deposit_required:
                single_loan.deposit_required = deposit_required
            requires_deposit_boolian: str = loan[LoanVO.REQUIRES_DEPOSIT_BOOLIAN]
            if requires_deposit_boolian:
                single_loan.requires_deposit_boolian = requires_deposit_boolian
            deposit_account: str = loan[LoanVO.DEPOSIT_ACCOUNT]
            if deposit_account:
                single_loan.deposit_account = deposit_account
            deposit_value: str = loan[LoanVO.DEPOSIT_VALUE]
            if deposit_value:
                single_loan.deposit_value = deposit_value
            deposit_profit: str = loan[LoanVO.DEPOSIT_PROFIT]
            if deposit_profit:
                single_loan.deposit_profit = deposit_profit
            deposit_sleep_duration: str = loan[LoanVO.DEPOSIT_SLEEP_DURATION]
            if deposit_sleep_duration:
                single_loan.deposit_sleep_duration = deposit_sleep_duration
            ratio_loan_to_deposit: str = loan[LoanVO.RATIO_LOAN_TO_DEPOSIT]
            if ratio_loan_to_deposit:
                single_loan.ratio_loan_to_deposit = ratio_loan_to_deposit
            maximum_payment_time: str = loan[LoanVO.MAXIMUM_PAYMENT_TIME]
            if maximum_payment_time:
                single_loan.maximum_payment_time = maximum_payment_time
            loan_Installment: str = loan[LoanVO.LOAN_INSTALLMENT]
            if loan_Installment:
                single_loan.loan_Installment = loan_Installment
            total_profit: str = loan[LoanVO.TOTAL_PROFIT]
            if total_profit:
                single_loan.total_profit = total_profit
            total_loan_profit: str = loan[LoanVO.TOTAL_LOAN_PROFIT]
            if total_loan_profit:
                single_loan.total_loan_profit = total_loan_profit
            separate_deposit: str = loan[LoanVO.SEPARATE_DEPOSIT]
            if separate_deposit:
                single_loan.separate_deposit = separate_deposit
            other_costs: str = loan[LoanVO.OTHER_COSTS]
            if other_costs:
                single_loan.other_costs = other_costs
            conditions: str = loan[LoanVO.CONDITIONS]
            if conditions:
                single_loan.conditions = conditions
            opportunity_cost_blocked: str = loan[LoanVO.OPPORTUNITY_COST_BLOCKED]
            if opportunity_cost_blocked:
                single_loan.opportunity_cost_blocked = opportunity_cost_blocked
            image: str = loan[LoanVO.IMAGE]
            if image:
                single_loan.image = image
            single_url: str = loan[LoanVO.SINGLE_URL]
            if single_url:
                single_loan.single_url = single_url
            rade_last_update_datetime: str = loan[LoanVO.RADE_LAST_UPDATE_DATETIME]
            if rade_last_update_datetime:
                single_loan.rade_last_update_datetime = rade_last_update_datetime
            profit_integer: int = loan[LoanVO.PROFIT_INTEGER]
            if profit_integer:
                single_loan.profit_integer = profit_integer
            max_loan_integer: int = loan[LoanVO.MAX_LOAN_INTEGER]
            if max_loan_integer:
                single_loan.max_loan_integer = max_loan_integer
            maximum_payment_time_integer: int = loan[LoanVO.MAXIMUM_PAYMENT_TIME_INTEGER]
            if maximum_payment_time_integer:
                single_loan.maximum_payment_time_integer = maximum_payment_time_integer
            is_active: bool = loan[LoanVO.IS_ACTIVE]
            if is_active:
                single_loan.is_active = is_active
            loan_type: str = loan[LoanVO.LOAN_TYPE]
            if loan_type:
                single_loan.loan_type = loan_type
            loan_type_id: str = loan[LoanVO.LOAN_TYPE_ID]
            if loan_type_id:
                single_loan.loan_type_id = loan_type_id
            last_update_datetime: str = loan[LoanVO.LAST_UPDATE_DATETIME]
            if last_update_datetime:
                single_loan.last_update_datetime = last_update_datetime

            return response_object

    @staticmethod
    def _create_list_search_response(loan_list: list[dict]):
        with start_span(op=LoggerVO.SEARCH_RESPONSE):
            response_object: GetLoansListResponse = GetLoansListResponse()

            for loan in loan_list:
                final_loan = response_object.loan_list.add()
                loan_id: str = loan[LoanVO.ID]
                if loan_id:
                    final_loan.loan_id = loan_id

                name: str = loan[LoanVO.NAME]
                if name:
                    final_loan.name = name
                maximum_payment_time: str = loan[LoanVO.MAXIMUM_PAYMENT_TIME]
                if maximum_payment_time:
                    final_loan.maximum_payment_time = maximum_payment_time
                interest_revenue: str = loan[LoanVO.INTEREST_REVENUE]
                if interest_revenue:
                    final_loan.interest_revenue = interest_revenue
                type_of_guarantee: str = loan[LoanVO.TYPE_OF_GUARANTEE]
                if type_of_guarantee:
                    final_loan.type_of_guarantee = type_of_guarantee
                max_loan: str = loan[LoanVO.MAX_LOAN]
                if max_loan:
                    final_loan.max_loan = max_loan
                loan_type: str = loan[LoanVO.LOAN_TYPE]
                if loan_type:
                    final_loan.loan_type = loan_type
                loan_type_id: str = loan[LoanVO.LOAN_TYPE_ID]
                if loan_type_id:
                    final_loan.loan_type_id = loan_type_id
                image: str = loan[LoanVO.IMAGE]
                try:
                    if image:
                        final_loan.image = image
                except:
                    pass

            return response_object

    @staticmethod
    def _create_related_search_response(loan_list: list[dict]):
        with start_span(op=LoggerVO.SEARCH_RESPONSE):
            response_object: GetRelatedListResponse = GetRelatedListResponse()

            for loan in loan_list:
                final_loan = response_object.related_loan.add()
                # loan_id: str = loan.get(ElasticSearchCommonsVO.ID)
                # if loan_id:
                #     final_loan.loan_id = loan_id

                # _source: Dict = loan.get(ElasticSearchCommonsVO.SOURCE)
                id: str = loan[LoanVO.ID]
                if id:
                    final_loan.loan_id = id
                image: str = loan[LoanVO.IMAGE]
                if image:
                    final_loan.image = image
                name: str = loan[LoanVO.NAME]
                if name:
                    final_loan.name = name
                interest_revenue: str = loan[LoanVO.INTEREST_REVENUE]
                if interest_revenue:
                    final_loan.interest_revenue = interest_revenue
                type_of_guarantee: str = loan[LoanVO.TYPE_OF_GUARANTEE]
                if type_of_guarantee:
                    final_loan.type_of_guarantee = type_of_guarantee
                max_loan: str = loan[LoanVO.MAX_LOAN]
                if max_loan:
                    final_loan.max_loan = max_loan
                maximum_payment_time: str = loan[LoanVO.MAXIMUM_PAYMENT_TIME]
                if maximum_payment_time:
                    final_loan.maximum_payment_time = maximum_payment_time
            return response_object
