# from ncl.dal.esdao.bulk_esdao import AbstractBulkESDao as BaseBulkESDao
# from ncl.utils.config.configuration import Configuration
# import pandas as pd
#
# from bzgi.config import BZSCConfig
#
#
# class MemberLog(BaseBulkESDao):
#     def __init__(self):
#         super().__init__()
#         BaseBulkESDao.__init__(self)
#         self.index_name = "v1_member_log_2022_8_"
#         self.index_name2 = "v1_member_log_2022_7_"
#
#     def search_logs(self, index_name: str):
#         query = {
#             "size": 1000
#         }
#         logs = self.search(index_name=index_name, query=query)
#         result = logs.get("hits").get("hits")
#         return result
#
#     def get_all_logs(self):
#         df = pd.DataFrame()
#
#         for i in range(1, 18):
#             all_events = []
#             index = self.index_name + str(i)
#             body_list = self.search_logs(index_name=index)
#
#             for body in body_list:
#                 gpsadid = body.get("_source").get("authentication").get("key")
#                 if gpsadid == "00000000-0000-0000-0000-000000000000":
#                     pass
#                 else:
#                     events = body.get("_source").get("event")
#                     for event in events:
#                         event["gps_add_id"] = gpsadid
#                         all_events.append(event)
#             data = pd.DataFrame(all_events)
#             frames = [df, data]
#             df = pd.concat(frames)
#         for i in range(28, 32):
#             all_events = []
#             index = self.index_name2 + str(i)
#             body_list = self.search_logs(index_name=index)
#
#             for body in body_list:
#                 gpsadid = body.get("_source").get("authentication").get("key")
#                 if gpsadid == "00000000-0000-0000-0000-000000000000":
#                     pass
#                 else:
#                     events = body.get("_source").get("event")
#                     for event in events:
#                         event["gps_add_id"] = gpsadid
#                         all_events.append(event)
#             data = pd.DataFrame(all_events)
#             frames = [df, data]
#             df = pd.concat(frames)
#         df.to_csv("member_log1", sep='\t')
#         return df
#
#
# if __name__ == '__main__':
#     from bzgi.config import BZSCConfig
#
#     Configuration.configure(BZSCConfig, alternative_env_search_dir=__file__)
#     MemberLog().get_all_logs()
