from bzscl.proto.bazdeh.media.car_price.car_price_pb2 import GetCarListResponse, GetCarListQuery
from bzscl.proto.bazdeh.media.car_price.car_price_pb2_grpc import CarPriceServeServicer

from sentry_sdk import start_span

from bzgi.model.vo.car_vo import CarVO
from bzgi.model.vo.logger_vo import LoggerVO
from bzgi.serve.logic.car_logic import CarLogic
from bzgi.utils.utils import Utils


class CarImp(CarPriceServeServicer):

    def __init__(self):
        super().__init__()
        self.utils = Utils()
        self.logic = CarLogic()

    def GetCarList(self, request: GetCarListQuery, context) -> GetCarListResponse:
        try:
            car_list = self.logic.get_car_list()
            response = self._create_list_search_response(car_list)
            return response
        except Exception as e:
            print(e)

    @staticmethod
    def _create_list_search_response(car_list: list[dict]):
        with start_span(op=LoggerVO.SEARCH_RESPONSE):
            response_object: GetCarListResponse = GetCarListResponse()

            for car in car_list:
                final_car = response_object.car.add()
                name: str = car[CarVO.NAME]
                if name:
                    final_car.name = name
                model_fa_name: str = car[CarVO.MODEL_FA_NAME]
                if model_fa_name:
                    final_car.model_fa_name = model_fa_name
                price: str = car[CarVO.PRICE]
                if price:
                    final_car.price = price
                production_year: str = car[CarVO.PRODUCTION_YEAR]
                if production_year:
                    final_car.production_year = production_year

                date: str = car[CarVO.DATE]
                if date:
                    final_car.date = date
                brand_fa_name: str = car[CarVO.BRAND_FA_NAME]
                if brand_fa_name:
                    final_car.brand_fa_name = brand_fa_name

            return response_object
