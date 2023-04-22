from apscheduler.schedulers.blocking import BlockingScheduler
from ncl.utils.common.singleton import Singleton

from bzgi.config import BZSCConfig
from bzgi.feed.logic.car_crawler_logic import CarCrawlerLogic
from bzgi.feed.logic.loan_crawler_logic import LoanCrawlerLogic


class Process(metaclass=Singleton):
    def __init__(self):

        self.laon_crawler_logic = LoanCrawlerLogic()
        self.car_crawler_logic = CarCrawlerLogic()
        self.scheduler = BlockingScheduler()

    def add_loans_to_elastic_with_bulk_scheduler(self):

        self.scheduler.add_job(self.laon_crawler_logic.save_loan_logic, "interval",
                               seconds=BZSCConfig.TIME_TO_SCHEDULE_LOANS)
        self.scheduler.print_jobs()
        self.scheduler.start()

    def add_cars_to_elastic_with_bulk_scheduler(self):

        self.scheduler.add_job(self.car_crawler_logic.save_cars, "interval",
                               seconds=BZSCConfig.TIME_TO_SCHEDULE_CARS)
        self.scheduler.print_jobs()
        self.scheduler.start()
