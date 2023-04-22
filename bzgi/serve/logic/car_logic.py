from ncl.logic.commons_base_logic import CommonsBaseLogic

import json

from ncl.utils.common.singleton import Singleton
from ncl.utils.config.configuration import Configuration

from bzgi.model.vo.car_vo import CarVO
from bzgi.model.vo.elastic_search_vo import ElasticSearchCommonsVO
from bzgi.model.vo.elastic_vo import ElasticVO
from bzgi.serve.dal.car_dal import CarDal


class CarLogic(CommonsBaseLogic, metaclass=Singleton):

    def __init__(self):
        super().__init__()
        self.dal = CarDal()

    def get_car_list(self):
        result = []
        car_cache = self.get_cars_from_cache()
        if not car_cache:
            self.do_cache_all_cars()
            car_cache = self.get_cars_from_cache()

        for key in car_cache:
            car = car_cache[key]
            car_dic = json.loads(car)
            result.append(car_dic)

        return result

    def get_cars_from_cache(self):
        return self.dal.get_all_car_info()

    def do_cache_all_cars(self):
        query = self._create_all_cars_query()
        dao_result = self.dal.search_post(query=query)
        for car in dao_result:
            single_car = self.create_single_car_model(car)
            self.do_cache_car_logic(single_car)

    @staticmethod
    def _create_all_cars_query():
        query = {
            "size": 1000
        }
        return query

    def create_single_car_model(self, car_model):
        car_id = car_model.get("_id")
        car_body = car_model.get("_source")
        car_dict = {}
        car_dict[CarVO.ID] = car_id

        car_dict[CarVO.NAME] = car_body.get(CarVO.NAME)
        car_dict[CarVO.MODEL_FA_NAME] = car_body.get(CarVO.MODEL_FA_NAME)
        car_dict[CarVO.PRICE] = car_body.get(CarVO.PRICE)
        car_dict[CarVO.PRODUCTION_YEAR] = car_body.get(CarVO.PRODUCTION_YEAR)
        car_dict[CarVO.DATE] = car_body.get(CarVO.DATE)
        car_dict[CarVO.BRAND_FA_NAME] = car_body.get(CarVO.BRAND_FA_NAME)

        return car_dict

    def do_cache_car_logic(self, car_single: dict = None):

        self.dal.do_cache(car_single)

if __name__ == '__main__':
    from bzgi.config import BZSCConfig

    Configuration.configure(BZSCConfig, alternative_env_search_dir=__file__)
    CarLogic().get_car_list()