import pandas as pd


class StockSimulator:
    def __init__(self, stocks, start_time, end_time):  # method 1
        self.stocks = stocks
        self.start_time = pd.to_datetime(start_time)  # save every time argument as datetime type
        self.end_time = pd.to_datetime(end_time)

        self.pricing = pd.read_csv('E:/pricing.csv')  # cutting off unnecessary data
        self.pricing = self.pricing.loc[self.pricing.tradingitemid.isin(self.stocks)]
        self.pricing = self.pricing.reset_index(drop='True')  # reset index
        self.split = pd.read_csv('E:/split.csv')
        self.split = self.split.loc[self.split.tradingitemid.isin(self.stocks)]
        self.split = self.split.reset_index(drop='True')
        self.dividend = pd.read_csv('E:/dividend.csv')
        self.dividend = self.dividend.loc[self.dividend.tradingitemid.isin(self.stocks)]
        self.dividend = self.dividend.reset_index(drop='True')

    def date_converter(self):  # method 2
        self.pricing.date = pd.to_datetime(self.pricing.date)
        self.split.date = pd.to_datetime(self.split.date)
        self.dividend.exdate = pd.to_datetime(self.dividend.exdate)

    def total_value(self):  # method 3
        self.date_converter()  # make every time variable to datetime type
        total_values = {}  # total_values has stock id as keys and
        # dictionary (where date for key and stock value for value) as values
        days = [d for d in self.pricing.date.tolist() if (d >= self.start_time) and (d <= self.end_time)]
        # list which contains target period only

        for s in self.stocks:
            pricing = self.pricing.loc[self.pricing.tradingitemid == s]
            split = self.split.loc[self.split.tradingitemid == s]
            dividend = self.dividend.loc[self.dividend.tradingitemid == s]
            values = {}  # date for key and stock value for value

            nums = {}  # date for key, the number of the stock for value
            num = self.stocks[s]  # initial number of the stock
            for d in days:  # construct nums
                if not split[split.date == d].empty:
                    num *= float(split[split.date == d].rate)
                nums[d] = num

            for d in nums:  # construct values, d == day
                n = nums[d]  # n == the number of stock
                price = float(pricing[pricing.date == d].close) * n

                if not dividend[dividend.exdate == d].empty:
                    divide = float(dividend[dividend.exdate == d].divamount) * n
                else:
                    divide = 0

                values[d] = price + divide

            total_values[s] = values  # construct total_values

            total_returns = {}  # which will be returned
            for d in days:
                value_sum = 0
                for s in total_values:
                    value_sum += total_values[s][d]
                total_returns[d] = value_sum
        return total_returns  # return dictionary type
