

import logging
import multiprocessing
from concurrent import futures
import grpc
from bzscl.proto.bazdeh.media.car_price import car_price_pb2_grpc
from bzscl.proto.bazdeh.media.loan import loan_pb2_grpc
import click
from ncl.utils.config.configuration import Configuration

from bzgi.model.vo.logger_vo import LoggerVO
from bzgi.process import Process


@click.group()
def cli():
    pass


@cli.command("start_scheduler")
def run_app_scheduler():
    Process().add_cars_to_elastic_with_bulk_scheduler()
    #Process().add_loans_to_elastic_with_bulk_scheduler()


_PROCESS_COUNT = multiprocessing.cpu_count()

@cli.command("run_grpc")
def serve_grpc():

    _THREAD_CONCURRENCY = _PROCESS_COUNT * BZSCConfig.GRPC_THREAD_PER_CPU_CORE

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=_THREAD_CONCURRENCY),
                         compression=grpc.Compression.NoCompression)
    _add_servicers_to_server(server)
    serve_url = BZSCConfig.GRPC_SERVE_HOST + ":" + str(BZSCConfig.GRPC_SERVE_PORT)
    logging.info(LoggerVO.STARTING_GRPC_SERVER.format(serve_url))
    server.add_insecure_port('%s' % serve_url)
    server.start()
    server.wait_for_termination()


def _add_servicers_to_server(server):
    '''
    Add all Servicers to Server
    '''
    from bzgi.serve.grpc.loan_imp import LoanImp
    from bzgi.serve.grpc.car_imp import CarImp
    loan_pb2_grpc.add_LoanServeServicer_to_server(LoanImp(), server)
    car_price_pb2_grpc.add_CarPriceServeServicer_to_server(CarImp(), server)




if __name__ == "__main__":
    from bzgi.config import BZSCConfig

    Configuration.configure(BZSCConfig, alternative_env_search_dir=__file__)

    cli()



