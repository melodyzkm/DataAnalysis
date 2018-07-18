# import re
# from config import *
# from pymongo import MongoClient, DESCENDING
#
# connection = MongoClient(BC_MONGODB, PORT_MONGODB)
# db = connection.bcf
#
# def check_dulplicated():
#
#     news  = db.news_collection.find({"data.source": "jinniu"}).sort('_id', DESCENDING)
#     count = 0
#     for new in news:
#
#
#         new_content = new.get("data").get("content")
#         try:
#             elif "】" in new_content:
#                 pattern_title = re.search(r'【(.*?)】', new_content)
#                 title = pattern_title[0]
#                 if db.news_collection.find_one({"data.content": {"$regex": title}, "data.source": {"$ne": "jinniu"}}):
#                     print("{} -- {} -- {}".format(str(count), "dulplicated".capitalize(), sub_content))
#                 else:
#                     print("{} -- {} -- {}".format(str(count), "No match", new_content))
#             elif new_content.count('，') >= 3:
#                 pattern_sub = re.search(r'，(.*?，.*?)，', new_content)
#                 sub_content = pattern_sub[1]
#
#                 dul_content= db.news_collection.find_one({"data.content": {"$regex": sub_content}, "data.source": {"$ne": "jinniu"}})
#                 if dul_content:
#                     print("{} -- {} -- {}".format(str(count), "dulplicated".capitalize(), dul_content.get("data").get('source')))
#
#                 else:
#                     print("{} -- {} -- {}".format(str(count), "No match", new_content))
#             elif:
#                 print("{} ** {}".format(str(count), new_content))
#         except Exception as e:
#             pass
#
#
#         count += 1
#
# if __name__ == "__main__":
#     # check_dulplicated()
#     with open("D:\\a\\a.txt", 'rt', encoding='utf-8') as f:
#         for line in f.readlines():
#             if "Dulplicate" not in line:
#                 print(line.strip())