from apscheduler.schedulers.blocking import BlockingScheduler
from ncl.utils.helper.commons_utils import CommonsUtils

from bzgi.feed.logic.loan_crawler_logic import LoanCrawlerLogic


class GilleteScheduler(CommonsUtils):
    def __init__(self):
        super().__init__()
        self.baazde_loan_crawl_logic=LoanCrawlerLogic()

        self.scheduler = BlockingScheduler()

    def do_gillete_scheduler(self):
        self.baazde_loan_crawl_logic.save_loan_logic()
        self.scheduler.add_job(self.baazde_loan_crawl_logic.save_loan_logic, 'interval',
                               seconds=60 * 60 * 24*50)
        self.scheduler.print_jobs()
        self.scheduler.start()