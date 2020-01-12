import pymysql
import logging
import company_list as company_list

# ------------------------- Set log level -------------------------
logging.basicConfig(level=logging.INFO)
# ------------------------- class - database ----------------------
class database():

    def __init__(self, host, user, passwd, database):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database
        self.connected = False

    def connect(self):
        try :
            self.con = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, database=self.database)
            logging.info(f"Connected to database.")
            self.connected = True
        except Exception as e :
            logging.error(f"Can't connect to database :: {str(e)}")
            self.connected = False

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
                logging.error(f"Can't get all table name :: {str(e)}")
        else :
            logging.info(f"Haven't connect to database, please connect to the database first.")
        return all_table_name

# ------------------------------ Load all excel into database (one time)-----------------------------

# 1. read adjust list



def main():
    Database = database("65.19.141.67", "mike8309_desktop", "Kb1990gb74P9795871570", "mike8309_stock")
    Database.connect()
    table_name = Database.get_all_table()
    print(table_name)
    symbol_list = company_list.Message(False).getSymbolList()
    print(symbol_list)

if __name__ == "__main__" :
    main()