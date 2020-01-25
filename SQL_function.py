#--------------------------
import os
import io
import time
import pandas
import random
import logging
import requests
import datetime
#-------------------------
import pymysql
from tqdm import tqdm
#-------------------------
import keys as keys
import company_list as company_list


# ------------------------- Set log level -------------------------
logging.basicConfig(level=logging.INFO)
# ------------------------- class - database ----------------------
class database():
    def __init__(self, host, database, mode):
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36"}
        self.host = host
        self.database = database
        self.connected = False
        self.mode = mode
        self.invalid_stock_key = ['OR', 'KEY', 'KEYS', 'OUT', 'ON', 'REAL', 'TRUE', 'ALL']

    def connect(self):
        try :
            for credential in keys.credential :
                try :
                    self.con = pymysql.connect(host=self.host, user=credential['username'], passwd=credential['password'], database=self.database)
                    logging.info(f"Connected to database.")
                    self.connected = True
                except Exception as e :
                    logging.info("Error when using {} to login  => {}".format(credential['username'], str(e)))
                    self.connected = False
                if self.connected :
                    logging.info("Login as {}".format(credential['username']))
                    return
            if not self.connected :
                logging.error(f"Lost Connection, wait for 60 seconds to reconnect.")
                time.sleep(60)
                self.connect()
        except Exception as e:
            raise Exception("Connection error : {}".format(str(e)))

    def get_all_table(self):
        all_table_name = []
        if self.connected :
            try :
                cur = self.con.cursor()
                query = "SELECT table_name FROM information_schema.tables Where TABLE_SCHEMA = 'mike8309_stock'"
                cur.execute(query)
                for row in cur:
                    all_table_name.append(row[0])
                cur.close()
            except Exception as e :
                if '1146' in str(e) :
                    logging.error(f"Symbol List does not exist in database. ")
                else :
                    logging.error(f"Can't get all table name :: {str(e)}")
        else :
            logging.info(f"Haven't connect to database, please connect to the database first.")
        return all_table_name

    def get_symbol_list(self) :
        status, msg= 1, []
        symbol_list = {'Symbol_list_NASDAQ' : [], 'Symbol_list_ETF' : []}
        if self.connected:
            for key in symbol_list.keys() :
                try:
                    cur = self.con.cursor()
                    query = "SELECT * FROM {}".format(key)
                    cur.execute(query)
                    for row in cur:
                        symbol_list[key].append(row[0])
                    cur.close()
                except Exception as e:
                    status = -1
                    msg.append(str(e))
                    if '1146' in str(e):
                        logging.error(f"Symbol List does not exist in database. ")
                    else:
                        logging.error(f"Can't get all table name :: {str(e)}")
        else:
            logging.info(f"Haven't connect to database, please connect to the database first.")
        return status, msg, symbol_list

    def Check_if_table_exist(self, tablename):
        match = False
        if self.connected :
            try :
                cur = self.con.cursor()
                query = f"SELECT count(TABLE_NAME) FROM information_schema.tables Where TABLE_SCHEMA = 'mike8309_stock' and TABLE_NAME = '{tablename}'"
                cur.execute(query)
                for row in cur:
                    if row[0] != 0 :
                        match = True
                cur.close()
            except Exception as e :
                logging.error(f"Can't Check if table exist :: {str(e)}")
        else :
            logging.info(f"Haven't connect to database, please connect to the database first.")
        return  match

    def execute_query(self, query):
        for part in query.split(";") :
            if len(part) > 0 :
                try :
                    cur = self.con.cursor()
                    cur.execute(part)
                    self.con.commit()
                    cur.close()
                    logging.info("Execution: {}".format(part))
                except Exception as e :
                    if '1062' in str(e) : # Duplicate
                        pass
                    elif '2013' in str(e) : # Lost connection
                        # reconnection
                        logging.error(f"Lost Connection, wait for 60 seconds to reconnect.")
                        time.sleep(60)
                        self.connect()
                        self.execute_query(query)
                    else :
                        msg = "Can't execute query : {} :: {}".format(part, str(e))
                        self.write_error(msg)

    def execute_select_query(self, table_name, select_column, num_of_item, condition_statement='', mode='') :
        if self.Check_if_table_exist(table_name) :
            result = []
            if mode == 'all' :
                query = 'SELECT {} FROM {}'.format('*', table_name) + condition_statement
            else :
                query = 'SELECT {} FROM {}'.format(','.join([item for item in select_column]), table_name) + " " + condition_statement
            try :
                cur = self.con.cursor()
                cur.execute(query)
                for row in cur :
                    temp = []
                    for index in range(num_of_item) :
                        temp.append(row[index])
                    result.append(temp)
                logging.info("Execution: {}".format(query))
            except Exception as e :
                if '2013' in str(e):  # Lost connection
                    # reconnection
                    logging.error(f"Lost Connection, wait for 60 seconds to reconnect.")
                    time.sleep(60)
                    self.connect()
                    self.execute_select_query(table_name, select_column, num_of_item, mode)
            if num_of_item == 1 :
                result = [x[0] for x in result]
            return result

    def get_lastest_date(self, table_name):
        date = ''
        query = "SELECT MAX(Date_) from {}".format(table_name)
        cur = self.con.cursor()
        cur.execute(query)
        for row in cur :
            date = row[0]
        return date

    def write_error(self, msg):
        logging.error(msg)
        with open("error.txt", 'a') as file:
            file.write(msg + "\n")
    def update_ddl_in_database(self, name, statement, mode):
        if mode == 'insert':
            query = "INSERT INTO DDL_statement (Name, Content) VALUES ('{}', '{}')".format(name, statement).replace(';', "\n")
        elif mode == 'update':
            query = "UPDATE DDL_statement SET Content = '{}' WHERE Name = '{}';".format(statement, name).replace(";", "\n")
        else :
            query = ''
            logging.error("update_ddl_in_database :: Unrecognized mode")
        self.execute_query(query)
    def start_processing(self, mod):
        if mod == 'offline' :
            self.offline()
        elif mod == 'online' :
            self.online()
        else :
            raise Exception("Can't recognize the mod :: {}".format(mod))

    def offline(self):

        # ---------------------------- Get symbol list from database -----------------------------------
        self.write_error(datetime.date.today().strftime("%Y-%m-%d") + "\n")
        # ------------------------------ Load all excel into database (one time)-------------------------
        # 1. Get symbol list from local,
        local_data = company_list.Message(False)
        symbol_list = local_data.getSymbolList()
        # 1.1 Check if all local DDL file are uploaded
        ddl_file_in_databaase = self.execute_select_query('DDL_statement', ['Name'], 1)
        for file in os.listdir(os.path.dirname(os.path.realpath(__file__)) + '\\dll') :
            name, tag = file.split('.')[0], file.split('.')[1]
            if tag == 'DLL' and name not in ddl_file_in_databaase:
                with open(os.path.dirname(os.path.realpath(__file__)) + "\\dll\\{}.DLL".format(name)) as f:
                    query = f.read().replace("\n", "").replace("\t", " ")
                self.update_ddl_in_database(name, query, 'insert')
            elif tag == 'DLL' :
                logging.info("DLL file : {}.DLL already exist in database.".format(name))
            else :
                # None DDL file
                pass

        # 2. Check if symbol list exist in database
        if not self.Check_if_table_exist('Symbol_list_NASDAQ') or not self.Check_if_table_exist('Symbol_list_ETF'):
            # a. Get DLL file and read query
            with open(os.path.dirname(os.path.realpath(__file__)) + "\\dll\\Symbol_list.DLL") as f:
                query = f.read().replace("\n", "").replace("\t", "")
            # b. Create table
            self.execute_query(query)
        else :
            logging.info("Table {} has already existed.".format("Symbol_list"))
        '''
        # 3. Load symbol into table
        
        for key_name_list in symbol_list :
            target_table = "Symbol_list_NASDAQ" if key_name_list == 'nasdaqlisted' else "Symbol_list_ETF"
            progress_bar = tqdm(total=len(symbol_list[key_name_list]))
            progress_bar.set_description(desc=f"Uploading {key_name_list} data")
            for element in symbol_list[key_name_list] :
                query = "INSERT INTO {} ({}) VALUES ('{}')".format(target_table, target_table.split("_")[-1], element)
                Database.execute_query(query)
                progress_bar.update()
            progress_bar.close()
        '''
        # 4. Create table for each stock and load data into it
        file_path = "E:\\stock\\data\\"
        with open(os.path.dirname(os.path.realpath(__file__)) + "\\dll\\Data_template.DLL") as f:
            query_table_temp = f.read().replace("\n", "")
        for key_name_list in symbol_list :
            sub_path = "stock\\" if key_name_list == 'nasdaqlisted' else 'etf\\'
            progress_bar = tqdm(total=len(symbol_list[key_name_list]))
            progress_bar.set_description(desc=f"Uploading {key_name_list} data")
            for element in symbol_list[key_name_list] :
                if element in self.invalid_stock_key or '.' in element :
                    continue
                return_code, data = local_data.getdata(file_path + sub_path, element + ".csv")
                if return_code == -1 :
                    self.write_error("Type : " + key_name_list + ", Name : " + element + ", Error : " + data + "\n")
                    continue
                # a. Create table
                if not self.Check_if_table_exist(element) :
                    query_table = query_table_temp.replace("TABLE_NAME", element)
                    self.execute_query(query_table)
                    date_cvs, date_datebase, start = 1, 0, 0
                else :
                    logging.info('Table {} already exist'.format(element))
                    # compare up to date or not
                    date_datebase = self.get_lastest_date(element)
                    date_cvs = data.iloc[data.shape[0] - 1][data.iloc[0].index[0]].replace("/", "-")
                    date_cvs = datetime.datetime(int(date_cvs.split("-")[0]), int(date_cvs.split("-")[1]),int(date_cvs.split("-")[2]))
                    if date_datebase == None :
                        date_cvs, date_datebase, start = 1, 0, 0
                    else :
                        date_datebase = datetime.datetime(date_datebase.year, date_datebase.month, date_datebase.day)
                # b. Load data into created table
                if return_code :
                    logging.info("starting ..............................")
                    if date_cvs > date_datebase :
                        try_list = [
                            str(date_datebase.year) + "/" + '{:02d}'.format(
                                date_datebase.month) + "/" + '{:02d}'.format(date_datebase.day),
                            str(date_datebase.year) + "/" + str(date_datebase.month) + "/" + str(date_datebase.day),
                            str(date_datebase.year) + "-" + '{:02d}'.format(
                                date_datebase.month) + "-" + '{:02d}'.format(date_datebase.day),
                            str(date_datebase.year) + "-" + str(date_datebase.month) + "-" + str(date_datebase.day)
                        ]
                        success = 0
                        for each in try_list:
                            try:
                                start = data['Date'].tolist().index(each)
                                success = 1
                            except:
                                pass
                        if success == 0:
                            raise Exception("Can't locate corresponding date in local file")
                        logging.info('Table {} needs to be updated => cvs : {}, database : {}, start={}, data.shape[0]={}'.format(element,str(date_cvs), str(date_datebase), start, str(data.shape[0])))
                        for i in range(start, data.shape[0]) :
                            row = data.iloc[i]
                            if int(row[row.index[0]][:4]) > 2015  :
                                query_insert_temp = "INSERT INTO TABLE_NAME (Date_, Open_, High, Low, Close_, Adj_close, Volume) VALUES ('{}', {}, {}, {}, {}, {}, {})".\
                                    format(row[row.index[0]], round(row[row.index[1]], 2), round(row[row.index[2]], 2), round(row[row.index[3]], 2),
                                           round(row[row.index[4]], 2), round(row[row.index[5]], 2), row[row.index[6]])
                                query_insert = query_insert_temp.replace("TABLE_NAME", element)
                                # Check if nan in query
                                if not 'nan' in query_insert :
                                    self.execute_query(query_insert)
                    elif date_cvs == date_datebase :
                        logging.info('Table {} are up to date'.format(element))
                    else :
                        logging.info('Need to download table {} to local'.format(element))
                progress_bar.update()
            progress_bar.close()

        self.close_connection()

    def online(self) :
        # ---------------------------------------- 1. Connect to database --------------------------------------------------
        # -------------------------------------- 2. Check if symbol list exist ---------------------------------------------
        status, msg, symbol_list = self.get_symbol_list()
        if status == -1 :
            logging.error(msg)
            raise Exception(msg)
        # 2. Check if symbol list exist
        #    a. Exist => step 3
        #    b. Not Exist => TBD
        #       I. Data exist
        #       II. Data not exist
        # -------------------------------------- 3. Download data for website ----------------------------------------------
        time_now = datetime.datetime.timestamp(datetime.datetime.today())
        time_now_timestamp = int(time_now - (time_now % 86400))
        query_table = self.execute_select_query('DDL_statement', ['Content'], 1, ' WHERE Name = "{}"'.format('Data_template'))[0].replace('\n', "")
        for key in symbol_list.keys() :
            progress_bar = tqdm(total=len(symbol_list[key]))
            progress_bar.set_description(desc=f"Uploading {key} data")
            for element in symbol_list[key] :
                if element in self.invalid_stock_key or '.' in element :
                    continue
                # 1. Get history data from server => Check if table exist
                if not self.Check_if_table_exist(element) : # Element table not exist
                    # Get table create query
                    query_table_temp = query_table.replace("TABLE_NAME", element)
                    self.execute_query(query_table_temp)
                    date_datebase = "2016-01-01"
                    date_datebase_timestamp = "1451606400"
                else : # Table exist => get latest date
                    logging.info('Table {} already exist'.format(element))
                    date_datebase = self.get_lastest_date(element)
                    date_datebase_time = datetime.datetime(date_datebase.year, date_datebase.month, date_datebase.day, 0, 0)
                    date_datebase_timestamp = int(datetime.datetime.timestamp(date_datebase_time))
                # 2. Download data for website
                #if time_now_timestamp != date_datebase_timestamp :
                if date_datebase != datetime.date.today():
                    url = "https://query1.finance.yahoo.com/v7/finance/download/" + element + "?period1={}&period2={}&interval=1d&events=history&crumb=PGwOHFfNgQt".format(date_datebase_timestamp, time_now_timestamp)
                    retry = 0
                    # 3. Compare downloaded and database's data
                    while retry < 6 :
                        try :
                            r = requests.post(url, self.headers)
                            data = pandas.read_csv(io.StringIO(r.content.decode('utf-8')))
                            logging.info("Download data from website :: {}".format(element))
                            retry = 10
                        except :
                            retry += 1
                            logging.error("Can't download data from website :: {}, {} try".format(element,retry))
                            time.sleep(10)
                    if retry == 6 :
                        self.write_error("Meet trying limit :: {}".format(element))
                        logging.error("Meet trying limit :: {}".format(element))
                    else :
                        # 5. Update database's data
                        logging.info("Table {} needs to be updated => from '{}' to '{}'".format(element, date_datebase, datetime.date.today()))
                        for index in range(data.shape[0]) :
                            row = data.iloc[index]
                            query_insert_temp = "INSERT INTO TABLE_NAME (Date_, Open_, High, Low, Close_, Adj_close, Volume) VALUES ('{}', {}, {}, {}, {}, {}, {})". \
                                format(row[row.index[0]], round(row[row.index[1]], 2), round(row[row.index[2]], 2),
                                       round(row[row.index[3]], 2),
                                       round(row[row.index[4]], 2), round(row[row.index[5]], 2), row[row.index[6]])
                            query_insert = query_insert_temp.replace("TABLE_NAME", element)
                            # Check if nan in query
                            if not 'nan' in query_insert:
                                self.execute_query(query_insert)
                    time.sleep(random.randint(100,200)*0.01)
                else :
                    logging.info('Table {} are up to date'.format(element))
                progress_bar.update()
            progress_bar.close()
        self.close_connection()

    def close_connection(self):
        self.con.close()

def main():
    Database = database("65.19.141.67", "mike8309_stock", 'offline')
    Database.connect()
    # ----------------------------- Daily update -----------------------------
    Database.start_processing('online')
    # -----------------------------
    '''
    status, msg, symbol_list = Database.get_symbol_list()
    if status == 1 :
        for key in symbol_list :
            for each_stock in symbol_list[key] :
                last_date = Database.get_lastest_date(each_stock) - datetime.timedelta(days=5)
                result = Database.execute_select_query(each_stock, ['Date_', 'Adj_close'], 2, "WHERE Date_ > '{}'".format(last_date))
                print(result)
    else :
        logging.error(msg)
    '''
if __name__ == "__main__" :
    main()


