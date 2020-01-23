import ftplib
import pandas
import os as os
from datetime import date
import pandas

class Message() :
    def __init__(self, update):
        self.url_stock = 'ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
        self.url_ETF = 'ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt'
        self.url_list = [self.url_stock, self.url_ETF]
        self.data = {}                          # { key : Dataframe }
        self.symbol_list_new = {}               # { key : list }
        self.adjust_symbol_list = {}            # { key : Dataframe }
        self.symbol_diff = {}                   # { key : Dataframe }
        self.filepath = "E:\\stock\\NameList\\"
        self.filename = [f"{url.split('/')[-1].split('.')[0]}.txt" for url in self.url_list]
        self.firsttime = True
        self.update = update
        self.Average_volumn_threshold = 500000

    def _check_status_firsttime(self):
        count_file = 0
        for index, name in enumerate(self.filename) :
            if os.path.isfile(self.filepath + self.filename[index]) :
                count_file +=1
        if count_file != 2 :
            print("Company_list :: First time execution!!")
            self.firsttime = True
            return
        self.firsttime = False

    def _start_download(self):
        for index, url in enumerate(self.url_list) :
            # connect to the server
            ftp = ftplib.FTP(url.split('/')[0])
            ftp.login()
            # switch to the directory containing the data
            ftp.cwd(url.split('/')[1])
            # now download to the desired path
            with open(self.filepath + self.filename[index], "wb") as file_handle:
                ftp.retrbinary("RETR " + self.filename[index], file_handle.write)
            self.data[self.filename[index]] = pandas.read_csv(self.filepath + self.filename[index] , sep="|")
            if not os.path.isfile(self.filepath + self.filename[index].split('.')[0] + ".csv") :
                self.data[self.filename[index]].to_csv(self.filepath + self.filename[index].split('.')[0] + ".csv", index = False)
            self.symbol_list_new[self.filename[index].split('.')[0]] = self.data[self.filename[index]][self.data[self.filename[index]].columns[0]].tolist()
            if self.firsttime == False :
                self.compare_list(self.filepath, self.data[self.filename[index]], self.filename[index], 0)
        print("Company_list :: Data downloading......")
        self._updata_list()
        #----------------------Froce update ----------------
        for index, url in enumerate(self.url_list):
            self.data[self.filename[index]].to_csv(self.filepath + self.filename[index].split('.')[0] + ".csv", index=False)
    def getdata(self, filepath, filename, name_column = 'all'):
        # return list
        # name_column = column name or all
        try :
            with open(filepath + filename, 'r') as fb:
                if name_column == 'all' :
                    data = pandas.read_csv(fb)
                else :
                    data = pandas.read_csv(fb)[name_column].tolist()
            return 1, data
        except Exception as e :
            print(f"[ERROR] Company_list :: getdata :: {e}")
            return -1, str(e)

    def compare_list(self, filepath, newData, name_oldData, index_comparecolumn):
        try :
            status, old_data = self.getdata(filepath, name_oldData.split(".")[0] + ".csv")
            if status == -1 :
                return
            symbol_diff = newData[~newData[newData.columns[index_comparecolumn]].isin(old_data[old_data.columns[index_comparecolumn]])]
            if name_oldData in self.filename :
                self.symbol_diff[name_oldData] = symbol_diff
            else :
                return symbol_diff
            #print("Company_list :: Data comparing......")
        except Exception as e :
            print(f"[ERROR] Company_list :: _compare_list :: {e}")

    def _updata_list(self):
        if not self.firsttime :
            update = 0
            for name in self.filename :
                if not self.symbol_diff[name].empty :
                    self.symbol_diff[name].to_csv(self.filepath + name, mode='a', header=False, index=False, na_rep='n/a', columns=self.symbol_diff[name].columns[:8])
                    self.symbol_diff[name].to_csv(self.filepath + "Newstock\\" + date.today().strftime("%y-%m-%d") + name.split('.')[0] + ".csv", mode='w', header=True if update == 0 else False, index=False, na_rep='n/a', columns=self.symbol_diff[name].columns[:8])
                    update = 1
        else :
            for name in self.filename :
                self.data[name].to_csv(self.filepath + name, mode='w',na_rep='n/a', columns=self.data[name].columns[:8])
        print("Company_list :: Data updating......")

    def update_list(self, dict):
        """
        Function :
            Write adjust list to a file(csv)
        Input :
            dict with two keys data in
        """
        #dict = pandas.DataFrame.from_dict(dict)
        for key in dict.keys() :
            file_name = key
            dict[key] = pandas.DataFrame(dict[key])
            dict[key].to_csv(self.filepath + "Adjust" + file_name + ".csv" , index=False, header = False)
        print("Company_list :: Adjust list updated")

    def update_all_list(self):
        filepath = "E:\\stock\\data\\"
        file_subpath = ['etf\\', 'stock\\']
        result = {'otherlisted' : [], 'nasdaqlisted' : []}
        for sub_path in file_subpath :
            path = 'otherlisted' if sub_path == 'etf\\' else 'nasdaqlisted'
            for file_name in os.listdir(filepath+sub_path) :
                if os.path.isfile(os.path.join(filepath + sub_path, file_name)) :
                    return_code, valumn_list = self.getdata(filepath + sub_path, file_name, 'Volume')
                    if sum(valumn_list[-100:])/100 > self.Average_volumn_threshold :
                        result[path].append(file_name.split('.')[0])
        self.update_list(result)

    def start_processing(self):
        #-----------------------Download date from web----------------------------
        if self.update :
            try :
                print("Company_list :: Start Updating data!!")
                self._check_status_firsttime()
                self._start_download()
                print("Company_list :: Complete Updating data!!")
            except Exception as e :
                print(f"[ERROR] Company_list :: start_processing :: {e}!!")
                print("Please use set Message(False) for getting old data!!")
        # -----------------------Load date from local----------------------------
        else :
            print("Company_list :: Load date from local!!")
            for filename in self.filename :
                data = pandas.read_csv(self.filepath + filename, sep="|")
                self.symbol_list_new[filename.split('.')[0]] = data[data.columns[0]].to_list()

    def getSymbolList(self):
        return_dict = {}
        for filename in self.filename :
            filename = filename.split('.')[0] + ".csv"
            if os.path.isfile(self.filepath + "Adjust" + filename) :
                return_dict[filename.split('.')[0]] = pandas.read_csv(self.filepath + "Adjust" + filename, header=None)
        if len(return_dict.keys()) == 2 :
            #print(f"Company_list :: Loadilng Adjust symbol list ")
            for key in return_dict :
                return_dict[key] = [','.join(x) for x in return_dict[key].values.tolist()]
                #print(return_dict[key])
            return return_dict
        else :
            print(f"Company_list :: Loadilng All symbol list ")
            return self.symbol_list_new

def main() :
    msg =  Message(True)
    msg.update_all_list()
    #msg.start_processing()
if __name__ == "__main__" :
    main()
