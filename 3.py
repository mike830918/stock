import company_list as company_list
import pandas
import datetime
from tqdm import tqdm
class function :

    def __init__(self):
        self.msg = company_list.Message(update=False)
        self.msg.start_processing()
        self.company_list = self.msg.getSymbolList()
    def updatedailyfluctuation(self) :
        '''
        Function :
            update list of daily fluctuation 1 day to 5days
        Input :
            type(str) : up / down
            range(int) : top n stocks
        return :
            int, list : status, [name, end price, in/decrease rate, in/decrease value]
        '''
        column_name = ["Symbol", "Today price", "Yesterday price", "rate", "value"]
        days = 5
        date = []
        outlistpf = [pandas.DataFrame(columns=column_name) for _ in range(days)]
        for key in self.company_list :
            file_path = "E:\stock\data\\" + ("etf" if key == "otherlisted" else "stock") + "\\"
            progress_bar = tqdm(total=len(self.company_list[key]))
            progress_bar.set_description(desc=f"Updating {key} data")
            for name in self.company_list[key] :
                status, data = self.msg.getdata(file_path, name + ".csv")
                for day in range(days) :
                    if status == 1 :
                        price_today = round(data.iloc[-(1+day)].at["Adj Close"], 3)
                        price_yeste = round(data.iloc[-(2+day)].at["Adj Close"], 3)
                        diff = round(price_today - price_yeste, 3)
                        ratial = round(diff/price_yeste, 4) * 100
                        outlistpf[day] = outlistpf[day].append(pandas.DataFrame([[name, price_today, price_yeste, ratial, diff]], columns=column_name), ignore_index=True)
                    else :
                        print(f"[ERROR] function :: getdailyfluctuation :: {name}")
                progress_bar.update()
            progress_bar.close()
        for index in range(days) :
            date.append(data.iloc[-(1+index)].at["Date"])
        for day in range(days) :
            outlistpf[day].sort_values(by='rate', inplace=True)
            outlistpf[day].to_csv("E:\stock\\function\daily_fluctuate\\" + f"{date[day]}.csv", mode='w', header=True, index=False)
x  = function()
x.updatedailyfluctuation()