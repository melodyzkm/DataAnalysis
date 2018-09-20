"""
The script is used to check the data quality which defined in xxx.json
@Author : Zhang Kai Ming
@Date: '2018-09-12 16:38:00.966214'
"""
import json
import pymssql
from collections import Counter


class MSSQL:
    """
    定义MSSQL类及查询方法
    """
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def _get_connect(self):
        if not self.db:
            raise(NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(self.host, self.user, self.pwd, self.db)
        cur = self.conn.cursor()
        if not cur:
            raise(NameError, "连接数据库失败")
        else:
            return cur

    def exec_query(self, sql):
        cur = self._get_connect()
        print(sql)
        cur.execute(sql)
        res_list = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return res_list

    def exec_nonquery(self, sql):
        cur = self._get_connect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


db_server = MSSQL("192.168.2.237", "sa", "Admin@123", "jydb")


def parse_json_to_dict(json_file):
    """
    将json文档转换为dict
    :param json_file: 需要转换的json文件路径
    :return: dict
    """
    str_json_content = ""
    with open(json_file, "rt", encoding="utf-8") as f:
        for line in f.readlines():
            line = line.strip()
            if not line.startswith("//"):
                str_json_content += line

    return json.loads(str_json_content)


def get_all_rows_from_table(table_name):
    """
    :param table_name:
    :return: table所有数据
    """
    res_list = db_server.exec_query("select top 100000 * from {}".format(table_name))
    i = 0
    while i < len(res_list):
        yield res_list[i]
        i += 1


def check_null_ratio(field, table):
    """
    检查null值占比
    :param field: 字段
    :param table: 表名
    :return: Null值比值
    """
    res_list = db_server.exec_query("select top 100000 {} from {}".format(field, table))
    amount_null = len([i for i in res_list if not i[0]])
    amount = len(res_list)
    return "{:.2%}".format(amount_null/amount)


def check_null_ratio_with_group_by(field, table, group_by_field, group_by_values):
    """
    检查null值占比
    :param field: 字段
    :param table: 表名
    :param group_by_field: group by 定义的字段
    :param group_by_values: group by 定义的值
    :return:
    """
    if len(group_by_values) == 1:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} is {}".format(field, table, group_by_field, group_by_values[0]))
        amount_null = len([i for i in res_list if not i[0]])
        amount = len(res_list)
    else:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} in ({})".format(field, table, group_by_field, ",". join(group_by_values)))
        amount_null = len([i for i in res_list if not i[0]])
        amount = len(res_list)
    return "{:.2%}".format(amount_null / amount)


def get_max_repeated_date(field, table):
    """
    检查max_repeated_date
    :param field:
    :param table:
    :return:
    """
    res_list = db_server.exec_query("select top 100000 {} from {} ".format(field, table))
    count = Counter(res_list)
    return count.most_common(1)[0][0][0]


def get_max_repeated_date_with_group_by(field, table, group_by_field, group_by_values):
    """
    检查max_repeated_date
    :param field:
    :param table:
    :param group_by_field:
    :param group_by_values:
    :return:
    """
    if len(group_by_values) == 1:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} is {}".format(field, table, group_by_field, group_by_values[0]))
    else:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} in ({})".format(field, table, group_by_field, ",".join(group_by_values)))
    count = Counter(res_list)
    return count.most_common(1)[0][0][0]


def get_duration_day(fields, table):
    """
    两个日期之间的间隔天数
    :param fields:
    :param table:
    :return:
    """
    durations = []
    res_list = db_server.exec_query("select top 100000 {} from {}".format(",".join(fields), table))
    for days in res_list:
        for i in range(len(days) - 1):
            if days[i] and days[i + 1]:
                durations.append((days[i + 1] - days[i]).days)

    if len(durations) > 0:
        return "{:.2f}".format(sum(durations)/len(durations))
    else:
        return 0


def get_duration_day_with_group_by(fields, table, group_by_field, group_by_values):
    """
    两个日期之间的间隔天数
    :param fields:
    :param table:
    :param group_by_field:
    :param group_by_values:
    :return:
    """
    durations = []
    if len(group_by_values) == 1:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} is {}".format(",".join(fields), table, group_by_field, group_by_values[0]))
    else:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} in ({})".format
                                        (",".join(fields), table, group_by_field, ','.join(group_by_values)))

    for days in res_list:
        for i in range(len(days) - 1):
            if days[i] and days[i + 1]:
                durations.append((days[i + 1] - days[i]).days)

    if len(durations) > 0:
        return "{:.2f}".format(sum(durations)/len(durations))
    else:
        return 0


def check_incorrect_ordered_ratio(fields, table):
    """
    乱序的记录值占比
    :param fields:
    :param table:
    :return: 乱序的记录值占比
    """
    durations = []
    res_list = db_server.exec_query("select top 100000 {} from {}".format(",".join(fields), table))
    for days in res_list:
        for i in range(len(days) - 1):
            if days[i] and days[i + 1]:
                durations.append((days[i + 1] - days[i]).days)

    if len(durations) > 0:
        unordered = [i for i in durations if i < 0]
        return "{:.2%}".format(len(unordered)/len(res_list))
    else:
        return 0


def check_incorrect_ordered_ratio_with_group_by(fields, table, group_by_field, group_by_values):
    """
    乱序的记录值占比
    :param fields:
    :param table:
    :param group_by_field:
    :param group_by_values:
    :return:
    """
    l_unordered = []
    if len(group_by_values) == 1:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} is {}".format(",".join(fields), table, group_by_field, group_by_values[0]))
    else:
        res_list = db_server.exec_query("select top 100000 {} from {} where {} in ({})".format
                                        (",".join(fields), table, group_by_field, ",".join(group_by_values)))

    for days in res_list:
        unordered = 0
        for i in range(len(days) - 1):
            if days[i] and days[i + 1]:
                if (days[i+1] - days[i]).days < 0:
                    unordered += 1
        l_unordered.append(unordered)

    if len(l_unordered) > 0:
        print(len([i for i in l_unordered if i > 0]))
        print(l_unordered)
        return "{:.2%}".format(len([i for i in l_unordered if i > 0])/len(l_unordered))
    else:
        return 0


def check_category_list(field, table):
    """
    取值范围
    :param field:
    :param table:
    :return:  如果list长度大于10， 使用省略号来表示
    """
    categorys = []
    res_list = db_server.exec_query("select distinct top 100000  {} from {} order by {} asc".format(field, table, field))
    for i in res_list:
        categorys.append(i[0])

    if len(categorys) <= 10:
        return categorys
    else:
        return categorys[:10] + ["..."]


def check_category_list_with_group_by(field, table, group_by_field, group_by_values):
    """
    取值范围
    :param field:
    :param table:
    :param group_by_field:
    :param group_by_values:
    :return:
    """
    categorys = []
    if len(group_by_values) == 1:
        res_list = db_server.exec_query("select distinct top 100000  {} from {} where {} is {} order by {} asc ".
                                        format(field, table, group_by_field, group_by_values[0], field))
    else:
        res_list = db_server.exec_query("select distinct top 100000  {} from {} where {} in ({}) order by {} asc ".
                                        format(field, table, group_by_field, ",".join(group_by_values), field))
    for i in res_list:
        categorys.append(i[0])

    if len(categorys) <= 10:
        return categorys
    else:
        return categorys[:10] + ["..."]


def get_statistics_value(field, table, s_type, group_by_field=None, group_by_values=None):
    if group_by_field and group_by_values:
        if len(group_by_values) == 1:
            res_list = db_server.exec_query("select top 100000 {} from {} where {} is {}".format(field, table, group_by_field, group_by_values[0]))
        else:
            res_list = db_server.exec_query("select top 100000 {} from {} where {} in ({})".format(field, table, group_by_field, ",".join(group_by_values)))

    else:
        res_list = db_server.exec_query("select top 100000 {} from {}".format(field, table))

    data = [float(i[0]) for i in res_list if i[0]]

    if data:
        if s_type == "avg":
            average = sum(data) / len(data)
            return "{:.2f}".format(average)
        elif s_type == "stdev":
            average = sum(data) / len(data)
            sdsq = sum([(i - average) ** 2 for i in data])
            stdev = (sdsq / (len(data))) ** 0.5
            return "{:.2f}".format(stdev)
    else:
        raise(ValueError, "No data fetched.")


def check_count_between_stdev(field, table, multiple, group_by_field=None, group_by_values=None):
    if group_by_field and group_by_values:
        if len(group_by_values) == 1:
            res_list = db_server.exec_query("select top 100000 {} from {} where {} is {}".format(field, table, group_by_field, group_by_values[0]))
        else:
            res_list = db_server.exec_query("select top 100000 {} from {} where {} in ({})".format(field, table, group_by_field, ",".join(group_by_values)))

    else:
        res_list = db_server.exec_query("select top 100000 {} from {}".format(field, table))

    data = [float(i[0]) for i in res_list if i[0]]
    if data:
        average = sum(data) / len(data)
        stdev = pow(sum([(i - average) ** 2 for i in data]), 0.5)
        between_data = [i for i in data if average - multiple * stdev <= i <= average + multiple * stdev]
        return "{:.2%}".format(len(between_data) / len(data))
    else:
        raise(ValueError, "No data fetched.")


def get_peak_value(field, table, p_type, group_by_field=None, group_by_values=None):
    if group_by_field and group_by_values:
        if len(group_by_values) == 1:
            res_list = db_server.exec_query("select top 100000 {} from {} where {} is {}".format(field, table, group_by_field, group_by_values[0]))
        else:
            res_list = db_server.exec_query("select top 100000 {} from {} where {} in ({})".format(field, table, group_by_field, ",".join(group_by_values)))

    else:
        res_list = db_server.exec_query("select top 100000 {} from {}".format(field, table))

    data = [float(i[0]) for i in res_list if i[0]]

    if data:
        if p_type == "max":
            return max(data)
        elif p_type == "min":
            return min(data)
        elif p_type == "med":
            return data[len(data)//2]
    else:
        raise(ValueError, "No data fetched.")


def item_check(check_type, check_items, table_name, group_by_field=None, group_by_values=None):
    if group_by_values and group_by_field:
        if check_type in ["duration_day", "incorrect_ordered_ratio"]:
            if check_type == "duration_day":
                return get_duration_day_with_group_by(check_items, table_name, group_by_field, group_by_values)
            elif check_type == "incorrect_ordered_ratio":
                return check_incorrect_ordered_ratio_with_group_by(check_items, table_name, group_by_field, group_by_values)
        else:
            values = []
            for item in check_items:
                if check_type == "null_ratio":
                    values.append(check_null_ratio_with_group_by(item, table_name, group_by_field, group_by_values))
                elif check_type == "max-repeated-date":
                    values.append(get_max_repeated_date_with_group_by(item, table_name, group_by_field, group_by_values))
                elif check_type == "category_list":
                    values.append(check_category_list_with_group_by(item, table_name, group_by_field, group_by_values))
                elif check_type == "max_value":
                    values.append(get_peak_value(item, table_name, "max", group_by_field, group_by_values))
                elif check_type == "min_value":
                    values.append(get_peak_value(item, table_name, "min", group_by_field, group_by_values))
                elif check_type == "median_value":
                    values.append(get_peak_value(item, table_name, "med", group_by_field, group_by_values))
                elif check_type == "average_value":
                    values.append(get_statistics_value(item, table_name, "avg", group_by_field, group_by_values))
                elif check_type == "stdev_value":
                    values.append(get_statistics_value(item, table_name, "stdev", group_by_field, group_by_values))
                elif check_type == "count_between_2stdev":
                    values.append(check_count_between_stdev(item, table_name, 2, group_by_field, group_by_values))
                elif check_type == "count_between_3stdev":
                    values.append(check_count_between_stdev(item, table_name, 3, group_by_field, group_by_values))
            return values

    else:
        if check_type in ["duration_day", "incorrect_ordered_ratio"]:
            if check_type == "duration_day":
                return get_duration_day(check_items, table_name)
            elif check_type == "incorrect_ordered_ratio":
                return check_incorrect_ordered_ratio(check_items, table_name)
        else:
            values = []
            for item in check_items:
                if check_type == "null_ratio":
                    values.append(check_null_ratio(item, table_name))
                elif check_type == "max-repeated-date":
                    values.append(get_max_repeated_date(item, table_name))
                elif check_type == "category_list":
                    values.append(check_category_list(item, table_name))
                elif check_type == "max_value":
                    values.append(get_peak_value(item, table_name, "max"))
                elif check_type == "min_value":
                    values.append(get_peak_value(item, table_name, "min"))
                elif check_type == "median_value":
                    values.append(get_peak_value(item, table_name, "med"))
                elif check_type == "average_value":
                    values.append(get_statistics_value(item, table_name, "avg"))
                elif check_type == "stdev_value":
                    values.append(get_statistics_value(item, table_name, "stdev"))
                elif check_type == "count_between_2stdev":
                    values.append(check_count_between_stdev(item, table_name, 2))
                elif check_type == "count_between_3stdev":
                    values.append(check_count_between_stdev(item, table_name, 3))

            return values


def main_check(json_path):
    l_results = []
    d = parse_json_to_dict(json_path)
    table_name = d.get("table_name")
    table_chinese_name = d.get("table_chinese_name")
    for item in d.get("columns"):
        columns = item.get("column_names")
        group_by_field = item.get("group_by") if "group_by" in item else None
        group_by_values = item.get("group_by_val") if "group_by_val" in item else None
        for ind in item.get("indicators"):
            check_type = ind.get("name")
            expect_result = ind.get("expected")
            results = item_check(check_type, columns, table_name, group_by_field, group_by_values)
            if isinstance(results, list):
                for i in range(len(results)):
                    l_results.append([table_name, table_chinese_name, columns[i], check_type, results[i], expect_result])
            else:
                l_results.append([table_name, table_chinese_name, columns, check_type, results, expect_result])

    log_path = json_path.replace(".json", "") + "_result.csv"
    with open(log_path, "wt", encoding="utf-8") as f:
        f.write(",".join(["表名称", "表中文名称", "检查字段", "检查项", "检查结果", "预期结果"]) + "\n")
        for res in l_results:
            f.write(",".join([str(i).replace(",", "，") for i in res]))
            f.write("\n")
    return l_results


if __name__ == "__main__":
    json_path = "demo.json"
    main_check(json_path)
    # s = get_duration_day_with_group_by(["InDate", "XGRQ"], "LC_LeaderIntroduce", "IfIn", ["0"])
    # print(s)
    # s = check_null_ratio("OffDate", "LC_LeaderIntroduce")
    # print(s)
    # ss = check_null_ratio_with_group_by("OffDate", "LC_LeaderIntroduce", "LeaderGender", ["1", "2"])
    # print(ss)