"""
This script is used to check if elastic search works normally.
Key words are used to validate the es's search function.
The result is restored in mongo DataMonitor.fundamental_infos_monitor.
The script is executed once a day.
"""

import datetime
from common.common import get_config, write_log_into_mongodb
from elasticsearch import Elasticsearch


def construct_query_body():
    query_bodies = []
    fundamental_key_words = ["消费升级", "产能拐点", "毛利率提升", "库存下降", "消费旺季", "首次覆盖", "需求增长", "持续放量", "淡季不淡", "订单充足", "产能释放"]
    for key_word in fundamental_key_words:
        body = {
                "size": 10,
                "query": {
                    "match_phrase": {
                        "doc_text": {
                            "query": key_word,
                            "slop": 5
                        }

                    }
                }

                ,
                "sort": {
                    "doc_publish_time": {
                        "order": "desc"
                    }
                }
            }
        query_bodies.append(body)
    return query_bodies


def get_search_result():
    config = get_config()
    es = Elasticsearch(config.get("ES_TEST"))
    bodies = construct_query_body()
    index_table = ["recomm-jyanalysereports-2018", "recomm-jymarketnews-2018"]
    query_result_doc_time = []
    for body in bodies:
        for info_source in index_table:
            query_result_doc_time += ([item.get("_source").get("doc_index_time") for item in es.search(index=info_source, body=body).get("hits").get("hits")])
    time_to_dt = [datetime.datetime.strptime(t.split('.')[0], "%Y-%m-%dT%H:%M:%S") for t in query_result_doc_time]
    current_time = datetime.datetime.now()
    start_time = current_time - datetime.timedelta(days=1)

    count = len([t for t in time_to_dt if t > start_time])
    d_check_result = {"create_time": datetime.datetime.now(), "24h_updated_amount": count}
    write_log_into_mongodb("fundamental_infos_monitor", d_check_result)


if __name__ == "__main__":
    print(get_search_result())