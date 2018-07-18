from common import  get_config
import xlrd, xlwt

from pymongo import MongoClient

config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf


def open_excel(file):
    try:
        data = xlrd.open_workbook(file)
        return data
    except Exception as e:
        print(e)


def read_excel(file, colnameindex=0, index=0):
    data = open_excel(file)
    table = data.sheets()[index]
    nrows = table.nrows #行数
    ncols = table.ncols #列数
    colnames =  table.row_values(colnameindex) #某一行数据
    list =[]
    for rownum in range(1,nrows):

         row = table.row_values(rownum)
         if row:
             app = {}
             for i in range(len(colnames)):
                app[colnames[i]] = row[i]
             list.append(app)
    return list

'''
市值排序	code	是否已有	通证缩写	将“否”单列出来	白皮书地址	相关概念	应用领域	项目动机	产品简介

'''
def write_excel():
    data = xlwt.Workbook()
    table = data.add_sheet('name')
    ll = read_excel("d:\\a\\markets.xlsx")
    codes = []
    for i in ll:
        codes.append((i.get('市值排序'), i.get('code'), i.get('是否已有')))

    for token in codes:
        order = token[0]
        code = token[1]
        ifYes = token[2]
        abbr = db_bcf.tokens.find_one({"code": code}).get("abbr") if db_bcf.tokens.find_one({"code": code}) else ""
        empty = ""
        white_paper_url = db_bcf.token_basic_infos.find_one({"code": code}).get('ico_whitepaper') if db_bcf.token_basic_infos.find_one({"code": code}) else ""
        project_conception = db_bcf.token_basic_infos.find_one({"code": code}).get('project_conception') if db_bcf.token_basic_infos.find_one({"code": code}) else " "
        project_application_area = db_bcf.token_basic_infos.find_one({"code": code}).get('project_application_area') if db_bcf.token_basic_infos.find_one({"code": code}) else " "
        project_motive_solution = db_bcf.token_basic_infos.find_one({"code": code}).get('project_motive_solution') if db_bcf.token_basic_infos.find_one({"code": code}) else " "
        project_product = db_bcf.token_basic_infos.find_one({"code": code}).get('project_product') if db_bcf.token_basic_infos.find_one({"code": code}) else " "

        row = [order, code, ifYes, abbr, empty, white_paper_url, project_conception, project_application_area, project_motive_solution, project_product]
        row_index = codes.index(token)
        for i in range(len(row)):
            print(row_index, i, row[i])
            table.write(row_index, i, row[i])
            i += 1

    data.save('d:\\a\\test.xls')


if __name__ == "__main__":
    # l = read_excel("d:\\a\\markets.xlsx")
    # print(l)
    print(write_excel())


