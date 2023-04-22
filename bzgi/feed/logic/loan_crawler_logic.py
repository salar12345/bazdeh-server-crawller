import requests

import re

from ncl.utils.common.singleton import Singleton
from ncl.utils.config.configuration import Configuration
from bzgi.feed.dal.loan_crawler_dal import LoanCrawlerDal
from bzgi.model.vo.logic_vo import LogicVOs
from bs4 import BeautifulSoup

from bzgi.model.vo.elastic_vo import ElasticVO
from bzgi.model.vo.loan_vo import LoanVO

from bzgi.utils.utils import Utils


class LoanCrawlerLogic(metaclass=Singleton):
    def __init__(self):
        super().__init__()
        self.dal = LoanCrawlerDal()

    loan_type_urls = [LogicVOs.marriage_url,
                      LogicVOs.student_url,
                      LogicVOs.urgent_url,
                      LogicVOs.cash_url,
                      LogicVOs.self_employment_url,
                      LogicVOs.buy_home_url,
                      LogicVOs.home_repair_url,
                      LogicVOs.buy_car_url,
                      LogicVOs.buy_commodity_url,
                      LogicVOs.installment_url,
                      LogicVOs.without_guarantor_url
                      ]

    def _multiple_replace(self, mapping, text=None):
        pattern = "|".join(map(re.escape, mapping.keys()))
        return re.sub(pattern, lambda m: mapping[m.group()], str(text))

    def convert_half_space(self, input_str):
        half_space_mapping = {
            "\u200c": " ", "\u200d": " ", "\u200e": " ", "\u200f": " ", "\ufeff": " ",
        }
        return self._multiple_replace(half_space_mapping, input_str)

    def convert_fa_to_en(self, input_str):
        fa_to_en_key_mapping = {
            "شرکت": LoanVO.COMPANY,
            "نام وام": LoanVO.LOAN_NAME,
            "سود سپرده": LoanVO.DEPOSIT_PROFIT,
            "سود": LoanVO.INTEREST_REVENUE,
            "سقف وام": LoanVO.MAX_LOAN,
            "کف مبلغ وام": LoanVO.LOAN_MIN,
            "نوع ضمانت": LoanVO.TYPE_OF_GUARANTEE,
            "مسدودی سپرده": LoanVO.BLOCKED_DEPOSIT,
            "مدت زمان مسدودی سپرده": LoanVO.DEPOSIT_TIME_LIMIT,
            "نیاز به سپرده": LoanVO.DEPOSIT_REQUIRED,
            "حساب سپرده لازم": LoanVO.DEPOSIT_ACCOUNT,
            "میزان سپرده": LoanVO.DEPOSIT_VALUE,
            "مدت زمان خواب سپرده": LoanVO.DEPOSIT_SLEEP_DURATION,
            "نسبت مبلغ وام به میزان سپرده": LoanVO.RATIO_LOAN_TO_DEPOSIT,
            "حداکثر زمان بازپرداخت": LoanVO.MAXIMUM_PAYMENT_TIME,
            "قسط وام": LoanVO.LOAN_INSTALLMENT,
            "کل سود وام": LoanVO.TOTAL_PROFIT,
            "مجموع رقم وام و سود": LoanVO.TOTAL_LOAN_PROFIT,
            "سپرده جداگانه": LoanVO.SEPARATE_DEPOSIT,
            "هزینه های جانبی": LoanVO.OTHER_COSTS,
            "شرایط و مقررات": LoanVO.CONDITIONS,
            "هزینه فرصت مسدودی": LoanVO.OPPORTUNITY_COST_BLOCKED
        }
        return self._multiple_replace(fa_to_en_key_mapping, input_str)

    def return_title(self, soup):
        line_of_title = soup.find_all("span", class_="post-title")
        title = [js.get_text() for js in line_of_title]
        return title[0]

    def return_last_update_datetime(self, soup):
        datetime = soup.find("time", class_="post-published updated")
        return datetime['datetime']

    def get_image_url(self, soup):
        images = []
        bodys = soup.find_all("a", class_="company-logo")
        for body in bodys:
            for image in body.find_all(src=True):
                images.append(image['src'])
        return images[0]

    def return_list_of_dictionary_values(self, soup):
        bodys = soup.find_all("td", class_="col-xs-8 col-lg-10")
        for i in bodys:
            for br in i.find_all("br"):
                br.replace_with("\n")
        # for i in bodys:
        #     body = i.select("div")
        items = [body.get_text() for body in bodys]
        for value in range(len(items)):
            items[value] = items[value].strip()
        return items

    def return_list_of_boolian_item_values(self, soup):
        bodys = soup.find_all("td", class_="col-xs-8 col-lg-10")
        boolian_items = []
        for body in bodys:
            x = body.select("i")
            if len(x):
                boolian_items.append(str(x[0]))
        for char in range(0, len(boolian_items)):
            if boolian_items[char][22] == 'r':
                boolian_items[char] = "false"
            else:
                boolian_items[char] = "true"
        return (boolian_items)

    def return_list_of_dictionary_keys(self, soup):
        bodys = soup.find_all("th", class_="col-xs-4 col-lg-2")
        # for i in bodys:
        #     body = i.select("div")
        # todo check body
        items = [js.get_text() for js in bodys]
        for item in range(len(items)):
            items[item] = self.convert_half_space(items[item])
            items[item] = self.convert_fa_to_en(items[item])

        return items

    def serve_single_loan_dict(self, single_url):
        single_dict = {
            LoanVO.NAME: None,
            LoanVO.COMPANY: None,
            LoanVO.LOAN_NAME: None,
            LoanVO.INTEREST_REVENUE: None,
            LoanVO.MAX_LOAN: None,
            LoanVO.LOAN_MIN: None,
            LoanVO.TYPE_OF_GUARANTEE: None,
            LoanVO.BLOCKED_DEPOSIT: None,
            LoanVO.BLOCKED_DEPOSIT_BOOLIAN: None,
            LoanVO.DEPOSIT_TIME_LIMIT: None,
            LoanVO.DEPOSIT_REQUIRED: None,
            LoanVO.REQUIRES_DEPOSIT_BOOLIAN: None,
            LoanVO.DEPOSIT_ACCOUNT: None,
            LoanVO.DEPOSIT_VALUE: None,
            LoanVO.DEPOSIT_PROFIT: None,
            LoanVO.DEPOSIT_SLEEP_DURATION: None,
            LoanVO.RATIO_LOAN_TO_DEPOSIT: None,
            LoanVO.MAXIMUM_PAYMENT_TIME: None,
            LoanVO.LOAN_INSTALLMENT: None,
            LoanVO.TOTAL_PROFIT: None,
            LoanVO.TOTAL_LOAN_PROFIT: None,
            LoanVO.SEPARATE_DEPOSIT: None,
            LoanVO.OTHER_COSTS: None,
            LoanVO.CONDITIONS: None,
            LoanVO.OPPORTUNITY_COST_BLOCKED: None,
            LoanVO.IMAGE: None,
            LoanVO.SINGLE_URL: None,
            LoanVO.RADE_LAST_UPDATE_DATETIME: None,
            LoanVO.PROFIT_INTEGER: 0,
            LoanVO.MAX_LOAN_INTEGER: 0,
            LoanVO.MAXIMUM_PAYMENT_TIME_INTEGER: 0,
            LoanVO.IS_ACTIVE: True
        }
        page = requests.get(single_url)
        soup = BeautifulSoup(page.content, "html.parser")
        single_dict[LogicVOs.name] = self.return_title(soup)
        single_dict[LogicVOs.image] = self.get_image_url(soup)
        single_dict[LogicVOs.single_url] = single_url
        single_dict[LoanVO.RADE_LAST_UPDATE_DATETIME] = self.return_last_update_datetime(soup)
        keys = self.return_list_of_dictionary_keys(soup)
        values = self.return_list_of_dictionary_values(soup)
        boolian_values = self.return_list_of_boolian_item_values(soup)
        for i in range(0, len(keys)):
            values[i] = values[i].replace("Warning! Better check yourself, you’re not looking too good.", '')
            if values[i] != "-":
                single_dict[keys[i]] = values[i]
        try:
            single_dict[LogicVOs.requires_deposit_boolian] = boolian_values[0]
            single_dict[LogicVOs.blocked_deposit_boolian] = boolian_values[1]
        except:
            pass
        try:
            if single_dict[LoanVO.INTEREST_REVENUE] is None:
                pass
            else:
                single_dict[LogicVOs.profit_integer] = int(single_dict[LoanVO.INTEREST_REVENUE].split(" ")[0])
        except:
            pass
        try:
            if single_dict[LoanVO.MAX_LOAN] is not None:
                single_dict[LogicVOs.max_loan_integer] = int(single_dict[LoanVO.MAX_LOAN].split(" ")[0])
        except:
            pass
        try:
            if single_dict[LoanVO.MAXIMUM_PAYMENT_TIME] is None:
                pass
            else:
                single_dict[LogicVOs.maximum_payment_time_integer] = int(
                    single_dict[LoanVO.MAXIMUM_PAYMENT_TIME].split(" ")[0])
        except:
            pass

        return single_dict

    def get_single_urls(self, loan_type_url):
        page = requests.get(loan_type_url)
        soup = BeautifulSoup(page.content, "html.parser")
        urlboxs = soup.select("tr td")
        urls = []
        for urlbox in urlboxs:
            for urlline in urlbox.find_all('a', href=True):
                url = urlline['href']
                urls.append(url)
        return (urls)

    def get_single_loans_list(self, page_url):
        single_loans_list = []
        for url in self.get_single_urls(page_url):
            single_loans_list.append(self.serve_single_loan_dict(url))

        return single_loans_list

    def save_loan_logic(self):
        urls = self.loan_type_urls
        all_loans = []
        for loan_type_url in range(0, 1):
            loan_dic = {}
            if loan_type_url == 0:
                loan_dic[LogicVOs.loan_type] = 'وام ازدواج'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.marriage_loan
            elif loan_type_url == 1:
                loan_dic[LogicVOs.loan_type] = 'وام دانشجویی'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.student_loan
            elif loan_type_url == 2:
                loan_dic[LogicVOs.loan_type] = 'وام فوری'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.urgent_loan
            elif loan_type_url == 3:
                loan_dic[LogicVOs.loan_type] = 'وام نقدی'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.cash_loan
            elif loan_type_url == 4:
                loan_dic[LogicVOs.loan_type] = 'وام خوداشتغالی'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.self_employment_loan
            elif loan_type_url == 5:
                loan_dic[LogicVOs.loan_type] = 'وام خرید مسکن'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.buy_home_loan
            elif loan_type_url == 6:
                loan_dic[LogicVOs.loan_type] = 'وام تعمیرات مسکن'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.home_repair_loan
            elif loan_type_url == 7:
                loan_dic[LogicVOs.loan_type] = 'وام خودرو'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.buy_car_loan
            elif loan_type_url == 8:
                loan_dic[LogicVOs.loan_type] = 'وام خرید کالا'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.buy_commodity_loan
            elif loan_type_url == 9:
                loan_dic[LogicVOs.loan_type] = 'خرید اقساطی'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.installment
            elif loan_type_url == 10:
                loan_dic[LogicVOs.loan_type] = 'وام بدون ضامن'
                loan_dic[LogicVOs.loan_type_id] = LogicVOs.without_guarantor_loan
            single_loans = self.get_single_loans_list(urls[loan_type_url])
            for single_loan in single_loans:
                single_loan.update(loan_dic)
                all_loans.append(single_loan)
                # self.gillete_dao.save_loan_from_crawl(loan_dict=single_loan)
        self._add_loan_to_elastic(all_loans)

    def _add_loan_to_elastic(self, loan_list):

        # query_body = self._create_last_date_time_body()
        # last_updated_post = self.dal.get_last_updated_post(query=query_body)
        #
        # expire_datetime_str = Utils.get_datetime_now()
        # if last_updated_post:
        #     expire_datetime_str: str = last_updated_post.get(LoanVO.LAST_UPDATE_DATETIME)
        #     if index := expire_datetime_str.find('+'):
        #         expire_datetime_str = expire_datetime_str[:index]

        current_datetime = Utils.get_datetime_now()
        for loan in loan_list:
            share_uri = loan[LoanVO.SINGLE_URL]
            uri = ''
            for i in range(28, len(share_uri)):
                if share_uri[i].isdigit():
                    uri = uri + share_uri[i]
                else:
                    break
            top_uri = ['14353', '6233', '9263', '13307', '20341', '7630', '13942', '13977', '19804', '13955', '5932',
                       '8108', '19257', '7700', '6327', '5930']
            top_uri_dict = {'14353': "90b9041a-a330-446d-9f6c-fe13a380c1a1",
                            '6233': "c32f2d1f-7899-4eed-815e-0d54423e9b58",
                            '9263': "6429e1b6-78f0-460f-9e74-ab8333bc0ee8",
                            '13307': "9d8387d9-c009-41ef-9dd9-1698080a46e4",
                            '20341': "37b40bed-da88-4f43-8d02-33516f80c46b",
                            '7630': "c9a0610b-839e-46b0-97d2-dae660fa0760",
                            '13942': "5a67fc82-6578-4b4f-9d71-0047c7757d16",
                            '13977': "78ad5f4f-4a45-4773-8696-40fb2b0d0bec",
                            '19804': "e15dea90-dcfb-4542-a1f9-8c27b3a619e0",
                            '13955': "525ee034-2dbd-48af-8316-20d595dfb5a4",
                            '5932': "63dea6c1-3766-48cb-923f-746baea41222",
                            '8108': "016ce712-d212-4223-97e7-249fbaaf761d",
                            '19257': "1d6c4020-8b57-434e-9e1e-a654449cd532",
                            '7700': "4c2459b6-7a47-4cc3-b876-f42c64317f39",
                            '6327': "114eeac8-3529-4774-b532-44b51741935c",
                            '5930': "8f1d5b3f-00fb-44c9-8ca1-b6a022b20dd2"
                            }
            if uri in top_uri:
                uri = top_uri_dict[uri]
            loan[LoanVO.LAST_UPDATE_DATETIME] = current_datetime
            self.dal.create_post(uri, loan)
            self.dal.update_post(uri, loan)
        self.dal.change_persist()
        # change_activate_qyery = self._create_expire_by_activate_query(expire_datetime_str)
        # self.dal.update_activity_state(change_activate_qyery)

    def _create_last_date_time_body(self):
        query = {ElasticVO.SIZE: 1,
                 ElasticVO.SORT: [
                     {
                         LoanVO.LAST_UPDATE_DATETIME: {
                             ElasticVO.ORDER: ElasticVO.DESC
                         }
                     }
                 ]
                 }
        return query

    def _create_expire_by_activate_query(self, expire_datetime_str):
        query = {
            "query": {
                "range": {
                    LoanVO.LAST_UPDATE_DATETIME: {
                        "lte": expire_datetime_str
                    }
                }
            },
            "script": {
                "source": "ctx._source['is_active'] = false",
                "lang": "painless"
            }
        }
        return query


if __name__ == '__main__':
    from bzgi.config import BZSCConfig

    Configuration.configure(BZSCConfig, alternative_env_search_dir=__file__)
    LoanCrawlerLogic().save_loan_logic()
