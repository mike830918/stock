import pandas as pandas
import io
import os
import requests
import company_list as company_list
from tqdm import tqdm
import time
import random
class Total_data() :
    def __init__(self, debug):
        #-----------------------------Controller setting--------------------------------
        self.Average_volumn_threshold = 500000
        self.Company_list_update = False
        self.new_company_list = {}
        self.debug = debug
        # ---------------------------- Get stock list ----------------------------------
        print("----------------------------------Get Symbol List Start--------------------------------")
        self.msg = company_list.Message(self.Company_list_update)
        self.msg.start_processing()
        self.company_list = self.msg.getSymbolList()
        self.error_company_list = {}
        print("----------------------------------Get Symbol List End----------------------------------")
        # ---------------------------- Set browser information -------------------------
        self.headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36"}
        # ---------------------------- Set parameter -----------------------------------
    def print(self,str):
        if self.debug :
            print(str)
    def downloadingdata(self):
        print("----------------------------------Download data Start----------------------------------")
        for stock_type in self.company_list :
            length_list = len(self.company_list[stock_type])
            progress_bar = tqdm(total=length_list)
            progress_bar.set_description(desc=f"Downloading {stock_type} data")
            for index, name in enumerate(self.company_list[stock_type]):
                retry = 0
                while retry < 4 :
                    try :
                        file_path = "E:\\stock\\data\\"
                        file_name = name + ".csv"
                        try :
                            url = "https://query1.finance.yahoo.com/v7/finance/download/" + file_name.split('.')[0] + "?period1=1411189200&period2=15689556000000&interval=1d&events=history&crumb=PGwOHFfNgQt"
                            r = requests.post(url,self.headers)
                            retry = 4
                        except Exception as e :
                            retry += 1
                            if retry < 4 :
                                self.print(f"[ERROR] Total_data :: downloadingdata :: Connect error {file_name} on {retry} attempt!!")
                                self.print("Wait 10 second for retry!!")
                                time.sleep(10)
                            else :
                                self.print(f"[ERROR] Total_data :: downloadingdata :: Connect error {file_name} reach limit times!!")
                                if stock_type not in self.error_company_list :
                                    self.error_company_list[stock_type] = [name]
                                else :
                                    self.error_company_list[stock_type].append(name)
                            continue
                        data = pandas.read_csv(io.StringIO(r.content.decode('utf-8')))
                        #------------------- Judge : Avergae volumn > 1m------------------------
                        if (sum(data["Volume"][-100:])/100) / self.Average_volumn_threshold > 1 :
                            if stock_type == "nasdaqlisted" :
                                subpath = "stock\\"
                            elif stock_type == "otherlisted" :
                                subpath = "etf\\"
                            else :
                                self.print("ERROR : Subpath not found!!")
                                break
                            file_path = file_path + subpath
                            #-------------------- Check if file already exist-------------------
                            if os.path.isfile(file_path + file_name):
                                #----------------Read old data----------------------------------
                                status, old_data = self.msg.getdata(file_path, file_name)
                                if status == -1 :
                                    self.print(f"[ERROR] Total_data :: downloadingdata :: Getting old data {file_name} Failure. ")
                                #----------------Compare missing data---------------------------
                                try :
                                    year = old_data["Date"][len(old_data)-1].split("/")[0]
                                    month = old_data["Date"][len(old_data)-1].split("/")[1]
                                    day = old_data["Date"][len(old_data)-1].split("/")[2]
                                    date = year + "-" + ("0" + month if int(month) < 10 else month) + "-" + ("0" + day if int(day) < 10 else day)
                                except :
                                    year = old_data["Date"][len(old_data)-1].split("-")[0]
                                    month = old_data["Date"][len(old_data)-1].split("-")[1]
                                    day = old_data["Date"][len(old_data)-1].split("-")[2]
                                    date = year + "-" + month + "-" + day
                                diff_index = pandas.Index(data["Date"]).get_loc(date)
                                diff_data = data[diff_index+1:len(data)]
                                #----------------Update data------------------------------------
                                diff_data.to_csv(file_path + file_name, mode='a', header=False, index=False)
                                self.print(f"{file_name} 更新完成。")
                            else:
                                data.to_csv(file_path + file_name, index = False)
                                self.print(f"{file_name} 寫入檔案。")
                            # Add qualified target to list then update
                            if stock_type not in self.new_company_list.keys() :
                                self.new_company_list[stock_type] = [name]
                            else :
                                self.new_company_list[stock_type].append(name)
                        else:
                            self.print(f"passing {name}")
                        time.sleep(random.randint(0,2))

                    except Exception as e :
                        retry = 4
                        self.print(f"[ERROR] Total_data :: downloadingdata :: {e} in {name}")
                        if stock_type not in self.error_company_list:
                            self.error_company_list[stock_type] = [name]
                        else:
                            self.error_company_list[stock_type].append(name)
                progress_bar.update()
            progress_bar.close()
        self.msg.update_list(self.new_company_list)
        print("----------------------------------Download data End------------------------------------")
def main() :
    download = Total_data(debug=False)
    download.downloadingdata()
if __name__ == "__main__" :
    main()

    '''
    x = company_list.Message(False)
    from os import listdir
    file_path = "E:\\stock\\data\\"
    file_name = {"stock": "nasdaqlisted", "etf" :"otherlisted"}
    for index,foldername in enumerate(listdir(file_path)):
        temp = []
        for name in listdir(file_path + foldername) :
            temp.append(name.split('.')[0])
        temp = pandas.DataFrame(temp)
        temp.to_csv("E:\\stock\\NameList\\" + "Adjust" + file_name[foldername] + ".csv", index=False, header=False)
        print(temp)
    
    from os import listdir
    file_path = "E:\\stock\\data\\"
    file_name = {"stock": "nasdaqlisted", "etf": "otherlisted"}
    temp = {}
    for index, foldername in enumerate(listdir(file_path)):
        for name in listdir(file_path + foldername):
            if foldername in temp :
                temp[foldername].append(name.split('.')[0])
            else :
                temp[foldername] = [name.split('.')[0]]
    print(temp)
    x.update_list(temp)
    '''