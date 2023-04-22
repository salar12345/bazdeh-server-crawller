
from ncl.dal.rddao.commons_base_rddao import CommonsBaseRDDao, RDKey
import json

from ncl.utils.common.singleton import Singleton


class CarCache(CommonsBaseRDDao, metaclass=Singleton):


    CAR_KEY = RDKey("car_price:{car_id}", ttl=60*60)

    def __init__(self) -> None:
        super().__init__()

    def do_cache(self, car: dict = {}):
        key = self.CAR_KEY.key.format(car_id=car.get('id'))
        value = json.dumps(car)
        self.client.set(name=key, value=value)
        self.client.expire(name=key, time=60*60)


    def get_all_car_info(self):
        keys = self.client.keys(pattern="car_price*")
        values = {}
        for key in keys:
            value = self.get_from_cache(key)
            values[key] = value
        return values

    def get_from_cache(self, car_id: str = None) -> list:
        car_info = self.client.get(name=car_id)
        return car_info