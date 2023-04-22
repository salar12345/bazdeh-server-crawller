from ncl.utils.common.singleton import Singleton

from bzgi.serve.dal.esdao.car_esdao import CarEsDao
from bzgi.serve.dal.rddao.car_rddao import CarCache


class CarDal(metaclass=Singleton):
    def __init__(self):
        self.esdao = CarEsDao()
        self.rddao = CarCache()


    def search_post(self, query: dict):
        return self.esdao.search_post(query)

    def get_all_car_info(self):
        return self.rddao.get_all_car_info()

    def do_cache(self, car_single: dict = None):
        self.rddao.do_cache(car_single)
