import requests
import re
from ncl.utils.common.singleton import Singleton
from ncl.utils.config.configuration import Configuration

from bs4 import BeautifulSoup

from bzgi.feed.dal.car_crawler_dal import CarCrawlerDal
from bzgi.model.vo.car_vo import CarVO
from bzgi.model.vo.elastic_search_vo import ElasticSearchCommonsVO
from bzgi.model.vo.html_vo import HtmlVO
from bzgi.utils.utils import Utils


class CarCrawlerLogic(metaclass=Singleton):
    def __init__(self):
        super().__init__()
        self.dal = CarCrawlerDal()
        self.url = CarVO.DIVAR_CAR_URL

    def make_soup_by_url(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        return soup

    def get_by_tag_and_class(self, soup, tag, class_name):
        bodys = soup.find_all(tag, class_=class_name)
        values = [js.get_text() for js in bodys]
        return values

    def _multiple_replace(self, mapping, text=None):
        pattern = "|".join(map(re.escape, mapping.keys()))
        return re.sub(pattern, lambda m: mapping[m.group()], str(text))

    def convert_to_en_numbers(self, input_str):
        half_space_mapping = {
            "۱": "1", "۲": "2", "۳": "3", "۴": "4", "۵": "5", "۶": "6", "۷": "7", "۸": "8", "۹": "9", "۰": "0",
            "١": "1", "٢": "2", "٣": "3", "٤": "4", "٥": "5", "٦": "6", "٧": "7", "٨": "8", "٩": "9", "٠": "0",
            ",": "", "،": ""
        }
        return self._multiple_replace(half_space_mapping, input_str)

    def make_dictionaries_list(self):
        soup = self.make_soup_by_url(self.url)
        now = Utils.get_datetime_now()
        names_list = self.get_by_tag_and_class(soup=soup, tag=HtmlVO.H3TAG, class_name=CarVO.NAME_CLASS)
        price_list = self.get_by_tag_and_class(soup=soup, tag=HtmlVO.PTAG, class_name=CarVO.VALUE_CLASS)
        other_list = self.get_by_tag_and_class(soup=soup, tag=HtmlVO.PTAG, class_name=CarVO.OTHER_CLASS)
        cars_list = []
        for i in range(0, len(names_list)):
            car_dict = {}
            car_dict[CarVO.NAME] = names_list[i]
            car_dict[CarVO.MODEL_FA_NAME] = names_list[i] + " " + self.convert_to_en_numbers(
                " ".join(other_list[i].split()[:-2]))
            car_dict[CarVO.PRICE] = int(self.convert_to_en_numbers(price_list[i].split()[0]))
            car_dict[CarVO.PRODUCTION_YEAR] = int(
                self.convert_to_en_numbers(other_list[i].split()[len(other_list[i].split()) - 1]))
            car_dict[CarVO.DATE] = now
            car_dict[CarVO.BRAND_FA_NAME] = names_list[i].split()[0]
            cars_list.append(car_dict)
        return cars_list

    def _add_cars_to_elastic(self, car_list):
        now = Utils.get_datetime_now()
        for car in car_list:
            car[CarVO.LAST_UPDATE_DATETIME] = now
            car[CarVO.PUBLISH_DATETIME] = now
            query = self._create_search_car_query(model_fa_name=car[CarVO.MODEL_FA_NAME],
                                                  production_year=car[CarVO.PRODUCTION_YEAR])

            if self.dal.search_post(query=query):
                doc_id = self.dal.search_post(query=query)[0].get('_id')
                self.dal.update_post(doc_id=doc_id, doc=car)
            else:
                doc_id = str(hash(car[CarVO.MODEL_FA_NAME] + str(car[CarVO.PRODUCTION_YEAR])))
                self.dal.create_post(doc_id=doc_id, doc=car)

        self.dal.change_persist()

    def _add_first_time_cars_to_elastic(self, car_list):
        now = Utils.get_datetime_now()
        for car in car_list:
            car[CarVO.LAST_UPDATE_DATETIME] = now
            car[CarVO.PUBLISH_DATETIME] = now
            query = self._create_search_car_query(model_fa_name=car[CarVO.MODEL_FA_NAME],
                                                  production_year=car[CarVO.PRODUCTION_YEAR])

            doc_id = str(hash(car[CarVO.MODEL_FA_NAME] + str(car[CarVO.PRODUCTION_YEAR])))
            self.dal.create_post(doc_id=doc_id, doc=car)

        self.dal.change_persist()

    def _create_search_car_query(self, model_fa_name, production_year):
        query = {
            ElasticSearchCommonsVO.QUERY: {
                ElasticSearchCommonsVO.BOOL: {
                    ElasticSearchCommonsVO.MUST: [
                        {
                            ElasticSearchCommonsVO.MATCH_PHRASE: {
                                "model_fa_name": model_fa_name
                            }
                        },
                        {
                            ElasticSearchCommonsVO.MATCH: {
                                "production_year": production_year
                            }
                        }
                    ]
                }
            }
        }
        return query

    def save_cars(self):
        car_list = self.make_dictionaries_list()
        try:
            self._add_cars_to_elastic(car_list)
        except:

            self._add_first_time_cars_to_elastic(car_list)


if __name__ == '__main__':
    from bzgi.config import BZSCConfig

    Configuration.configure(BZSCConfig, alternative_env_search_dir=__file__)
    CarCrawlerLogic().save_cars()
