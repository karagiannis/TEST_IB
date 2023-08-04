import datetime
import os
import pytz
import csv
import time
import threading  # Import threading module for multithreading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract


class MyWrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.contract_map = {}
        self.data_received = False
        self.line_number = 1  # Initialize line number to 1

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
        print("Received historical data:", bar)

        # If reqId is -1, it means this is not a specific historical data request,
        # but rather a response related to the data farm connection
        if reqId == -1:
            print("Data farm connection is OK:", bar)

        # If reqId is not -1, it means this is a response to a specific historical data request
        # and you can handle the OHLC data accordingly
        else:
            if reqId in self.contract_map:
                self.contract_map[reqId]['data'].append(bar)

    def historicalDataEnd(self, reqId, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId)
        self.data_received = True


def find_first_saturday_in_january_2005():
    # Find the first Saturday in January 2005
    start_date = datetime.datetime(2005, 1, 1)
    while start_date.weekday() != 5:  # 5 represents Saturday (Monday is 0, Sunday is 6)
        start_date += datetime.timedelta(days=1)
    return start_date


class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper  # Store the instance of MyWrapper

    def historical_data_worker():
        """
        Worker function for running the client event loop in a separate thread.
        """
        while not wrapper.data_received:
            client.run()  # Process IB messages
            time.sleep(0.1)

    def request_data(self, contract, pair):
        eastern_tz = pytz.timezone('US/Eastern')

        # Find the first Saturday in January 2005
        start_date = find_first_saturday_in_january_2005()

        while start_date.strftime("%Y%m%d") <= datetime.datetime.now(eastern_tz).strftime("%Y%m%d"):
            # Calculate the end date as the next Friday (5 trading days after Saturday)
            end_date = start_date + datetime.timedelta(days=5)

            # Set the endDateTime to the end of the day on the next Saturday 00:00:00
            end_date_time = end_date.replace(hour=00, minute=00, second=00).strftime("%Y%m%d %H:%M:%S") + 'US/Eastern'

            reqId = len(self.wrapper.contract_map) + 1
            self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}
            print(
                f"Requesting data for {contract.symbol} {contract.currency} from {start_date.strftime('%Y%m%d %H:%M:%S')} to {end_date_time} with a bar size of 1 day.")
            self.reqHistoricalData(reqId, contract, end_date_time, "5 D", "1 day", "MIDPOINT", 1, 1, False, [])
            time.sleep(3)  # Wait for 1 second before making another request

            # Find the next Saturday
            start_date += datetime.timedelta(days=7)

            # Break the loop for now to check if data is being saved
            break

        print("Request loop completed.")


wrapper = MyWrapper()
client = MyClient(wrapper)


def main():
    global wrapper, client
    # wrapper = MyWrapper()
    # client = MyClient(wrapper)
    client.connect("127.0.0.1", 7497, clientId=0)
    # Check if connected
    print(f"Connecting to IB Gateway/TWS. Is connected: {client.isConnected()}")

    # Define major currency pairs
    majors = [
        (Contract(), "EURUSD"),
        (Contract(), "GBPUSD"),
        (Contract(), "USDJPY"),
        (Contract(), "USDCHF"),
        (Contract(), 'AUDUSD'),
        (Contract(), "USDCAD"),
        (Contract(), "NZDUSD"),
        (Contract(), "EURGBP")
    ]
    # Start the historical data worker thread
    historical_data_thread = threading.Thread(target=client.historical_data_worker)
    historical_data_thread.start()

    for contract, pair in majors:
        contract.symbol = pair[:3]
        contract.currency = pair[3:]
        contract.exchange = "IDEALPRO"
        contract.secType = "CASH"  # Specify the security type
        client.request_data(contract, pair)

    while not wrapper.data_received:
        client.run()  # Process IB messages
        time.sleep(0.1)

        # Wait for the historical data thread to complete
    historical_data_thread.join()


if __name__ == "__main__":
    main()
