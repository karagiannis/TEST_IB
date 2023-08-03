import datetime
import os
import pytz
import csv
import time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract


class ForexDataApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.contract_map = {}
        self.data_received = False

    def find_first_friday_in_january_2005(self):
        # Find the first Friday in January 2005
        start_date = datetime.datetime(2005, 1, 1)
        while start_date.weekday() != 4:  # 4 represents Friday (Monday is 0, Sunday is 6)
            start_date += datetime.timedelta(days=1)
        return start_date

    def request_data(self, contract, duration_str, filename):
        eastern_tz = pytz.timezone('US/Eastern')
        end_date_time = datetime.datetime.now(eastern_tz).strftime("%Y%m%d %H:%M:%S")

        # Calculate the start date based on the duration
        duration = int(duration_str.split()[0])  # Extract the number from the duration_str

        # Find the first Friday in January 2005
        start_date = self.find_first_friday_in_january_2005()

        while start_date.strftime("%Y%m%d") <= datetime.datetime.now(eastern_tz).strftime("%Y%m%d"):
            reqId = len(self.contract_map) + 1
            self.contract_map[reqId] = {'data': [], 'contract': contract}
            print(f"Requesting data for {contract.symbol} {contract.currency} from {start_date.strftime('%Y%m%d %H:%M:%S')} to {end_date_time} with a bar size of 1 day.")
            self.reqHistoricalData(reqId, contract, start_date.strftime("%Y%m%d %H:%M:%S") + " US/Eastern", duration_str, "1 day", "MIDPOINT", 1, 1, False, [])
            time.sleep(1)  # Wait for 1 second before making another request

            # Find the next Friday
            start_date += datetime.timedelta(days=7)

            # Save data to CSV
            self.save_data_to_csv(reqId, filename)

    def save_data_to_csv(self, reqId, filename):
        data_folder = "./historicaldata/forex"
        os.makedirs(data_folder, exist_ok=True)
        file_path = os.path.join(data_folder, f"{filename}.csv")

        with open(file_path, mode='a', newline='') as file:
            writer = csv.writer(file)

            # Append the historical data to the file
            for data_point in self.contract_map[reqId]['data']:
                writer.writerow([self.contract_map[reqId]['contract'].symbol,
                                 self.contract_map[reqId]['contract'].currency,
                                 data_point.date,
                                 data_point.open,
                                 data_point.high,
                                 data_point.low,
                                 data_point.close,
                                 data_point.volume])

    def historicalData(self, reqId, bar):
        if reqId in self.contract_map:
            self.contract_map[reqId]['data'].append(bar)

    def historicalDataEnd(self, reqId, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId)
        self.disconnect()
        self.data_received = True


if __name__ == "__main__":
    app = ForexDataApp()
    app.connect("127.0.0.1", 7497, clientId=1)  # Replace with your IB Gateway/TWS connection details

    # Define major currency pairs
    majors = [
        (Contract(), "EURUSD"),
        (Contract(), "GBPUSD"),
        (Contract(), "USDJPY"),
        (Contract(), "USDCHF"),
        (Contract(), "AUDUSD"),
        (Contract(), "USDCAD"),
        (Contract(), "NZDUSD"),
        (Contract(), "EURGBP")
    ]

    for contract, filename in majors:
        contract.symbol = filename[:3]
        contract.currency = filename[3:]
        contract.exchange = "IDEALPRO"
        app.request_data(contract, "30 D", filename)

    while not app.data_received:
        app.run()  # Process IB messages
        time.sleep(0.1)
