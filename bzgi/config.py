from typing import Dict

from ncl.utils.config.base_config import CommonsBaseConfig


class BZSCConfig(CommonsBaseConfig):
    SQLALCHEMY_DATABASE_URI = None
    LOAN_INDEX_NAME: str = "v1_loan"
    CAR_INDEX_NAME: str = "v1_car"
    LOAN_INDEX_NAME_ALIAS: str = "loan"
    TIME_TO_SCHEDULE_LOANS: int = 60 * 60 * 24 * 7
    TIME_TO_SCHEDULE_CARS: int = 60 * 60 * 24
    ELASTIC_SEARCH_BATCH_INTERVAL_THRESHOLD_IN_SECONDS: int = 1
    ELASTIC_SEARCH_BATCH_DOC_COUNT_THRESHOLD: int = 500
    ELASTIC_SEARCH_KWARG: Dict = {"maxsize": 25, "http_compress": True}
    GRPC_SERVE_HOST = "0.0.0.0"
    GRPC_SERVE_PORT = 8105
