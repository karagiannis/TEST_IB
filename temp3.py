import datetime
import os
import sys
import pytz
import csv
import time
import threading  # Import threading module for multithreading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import pandas as pd

global_bar_size = None  # Initialize the global_bar_size variable

class MyWrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.contract_map = {}
        self.data_received = False
        self.line_number = 1  # Initialize line number to 1

    def save_data_to_csv(self, reqId, filename, bar_size):
        data_folder = "./historical_data/forex"
        os.makedirs(data_folder, exist_ok=True)

        contract = self.contract_map[reqId]['contract']
        currency_pair = f"{contract.symbol}{contract.currency}"
        currency_pair_folder = os.path.join(data_folder, currency_pair)
        os.makedirs(currency_pair_folder, exist_ok=True)

        # Map bar sizes to their corresponding suffixes
        bar_size_suffix = {
            "1 secs": "1_secs",
            "5 secs": "5_secs",
            "10 secs": "10_secs",
            "15 secs": "15_secs",
            "30 secs": "30_secs",
            "1 min": "1_min",
            "2 mins": "2_mins",
            "3 mins": "3_mins",
            "5 mins": "5_mins",
            "10 mins": "10_mins",
            "15 mins": "15_mins",
            "20 mins": "20_mins",
            "30 mins": "30_mins",
            "1 hour": "1_hour",
            "2 hours": "2_hours",
            "3 hours": "3_hours",
            "4 hours": "4_hours",
            "8 hours": "8_hours",
            "1 day": "1_day",
            "1 week": "1_week",
            "1 month": "1_month"
        }

        # Check if the specified bar size exists in the mapping
        if bar_size in bar_size_suffix:
            # Add the bar size suffix to the filename
            filename_with_suffix = f"{filename}_{bar_size_suffix[bar_size]}.csv"
        else:
            # If the bar size is not found, use a default suffix
            filename_with_suffix = f"{filename}_default.csv"

        file_path = os.path.join(currency_pair_folder, filename_with_suffix)

        if os.path.exists(file_path):
            # Load existing data and append new data
            try:
                existing_data = pd.read_csv(file_path)

                # Convert the 'Date' column in existing_data to datetime objects
                existing_data['Date'] = pd.to_datetime(existing_data['Date'])

                new_data = pd.DataFrame([
                    [pd.to_datetime(data_point.date), data_point.open, data_point.high, data_point.low,
                     data_point.close, data_point.volume]
                    for data_point in self.contract_map[reqId]['data']
                ], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

                combined_data = pd.concat([existing_data, new_data])
                combined_data.drop_duplicates(subset=['Date'], keep='first', inplace=True)
                combined_data.sort_values(by='Date', inplace=True)

                combined_data.to_csv(file_path, index=False)
            except Exception as e:
                print(f"Error occurred while appending data to the CSV file: {str(e)}")
        else:
            # Create a new file and write the data
            try:
                with open(file_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    for data_point in self.contract_map[reqId]['data']:
                        writer.writerow([data_point.date, data_point.open, data_point.high, data_point.low,
                                         data_point.close, data_point.volume])
            except Exception as e:
                print(f"Error occurred while creating and writing data to the CSV file: {str(e)}")

    def check_data_order(self, currency_pair_folder):
        data_files = os.listdir(currency_pair_folder)
        dates = [int(file_name.split('_')[1].split('.')[0]) for file_name in data_files]
        dates.sort()

        for i in range(1, len(dates)):
            prev_date = dates[i - 1]
            curr_date = dates[i]
            if curr_date - prev_date > 1:
                print(f"You are missing dates {prev_date + 1} to {curr_date - 1}")

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
        # Since historicalDataEnd is called after all historical data is received, we can save the data to CSV here.
        contract = self.contract_map[reqId]['contract']
        filename = f"{contract.symbol}{contract.currency}"
        self.save_data_to_csv(reqId, filename, global_bar_size)


    def find_first_friday(self, approximate_start_date):
        # Find the first friday from an approximate given start date
        start_date = datetime.datetime.strptime(approximate_start_date, "%Y%m%d")  # Convert to datetime object
        while start_date.weekday() != 4:  # 4 represents Friday (Monday is 0, Sunday is 6)
            start_date += datetime.timedelta(days=1)
        return start_date


class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper  # Store the instance of MyWrapper

    def historical_data_worker(self):
        """
        Worker function for running the client event loop in a separate thread.
        """
        while not self.wrapper.data_received:  # Use 'self.wrapper.data_received' to access the instance variable
            self.run()  # Process IB messages
            time.sleep(0.1)
            # Wait for the historical data thread to complete
            historical_data_thread.join()

    def request_data(self, contract, pair, approx_start_date, bar_size):
        eastern_tz = pytz.timezone('US/Eastern')

        # We will request OHLC bar data from the friday near approx_start_date and take 5 tradings days of OHLC bar
        # date backwards and increment the "friday" until "friday" is less or equal to date now
        now = datetime.datetime.now(eastern_tz).strftime("%Y%m%d")

        # Find the first friday and make it the end date. You will request 5 trading days of OHLC data including this
        # friday
        friday = self.wrapper.find_first_friday(approx_start_date)
        end_date = friday
        end_date_time = end_date.replace(hour=15, minute=15, second=00).strftime("%Y%m%d %H:%M:%S") + ' US/Eastern'

        # Create an unique request ID required for the API
        reqId = len(self.wrapper.contract_map) + 1

        # Associate data and contract to that particular request ID
        self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}

        print(
            f"Requesting data for {contract.symbol} {contract.currency}"
            f"from {(friday - datetime.timedelta(days=4)).strftime('%Y%m%d %H:%M:%S')} to {friday} "
            f"with a bar size of "+bar_size)

        # Make the first data request of 5 bars
        self.reqHistoricalData(reqId, contract, end_date_time, "5 D", bar_size,
                               "MIDPOINT", 1, 1, False, [])
        time.sleep(3)  # Wait for 3 seconds before making another request

        while (end_date + datetime.timedelta(days=6)).strftime("%Y%m%d") <= now:
            end_date = end_date + datetime.timedelta(days=6)  # the next friday forward in time
            # We should have a unique request ID for each new request
            reqId += 1

            # Associate data and contract to that particular request ID
            self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}

            # Set the endDateTime to the end of the day on the next Friday
            end_date_time = end_date.replace(hour=15, minute=15, second=00).strftime("%Y%m%d %H:%M:%S") + ' US/Eastern'

            print(f"Requesting data for {contract.symbol} {contract.currency}"
                  f"from {(end_date - datetime.timedelta(days=4)).strftime('%Y%m%d %H:%M:%S')} to "
                  f"{end_date_time} with a bar size of 1 day.")

            # Make the data request of 5 bars for the next period
            self.reqHistoricalData(reqId, contract, end_date_time, "5 D", "1 day",
                                   "MIDPOINT", 1, 1, False, [])
            time.sleep(3)  # Wait for 3 seconds before making another request

        print("Finished requesting data.")
        self.wrapper.data_received = True  # Update the flag when all data has been requested.


wrapper = MyWrapper()
client = MyClient(wrapper)
historical_data_thread = None


def validate_bar_size(bar_size):
    valid_bar_sizes = [
        "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
        "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
        "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
        "3 hours", "4 hours", "8 hours", "1 day", "1 week",
        "1 month"
    ]
    return bar_size in valid_bar_sizes


def validate_bar_size(bar_size):
    valid_bar_sizes = [
        "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
        "1 min", "2 mins", "3 mins", "5 mins", "10 mins",
        "15 mins", "20 mins", "30 mins", "1 hour", "2 hours",
        "3 hours", "4 hours", "8 hours", "1 day", "1 week",
        "1 month"
    ]
    return bar_size in valid_bar_sizes


def main():
    approximate_start_date = None
    bar_size = None

    if len(sys.argv) == 4:
        approximate_start_date = sys.argv[1]
        bar_size_string = sys.argv[2] + " " + sys.argv[3]

    else:
        print("Usage: python temp3.py <approximate_start_date> <bar_size>")
        print("Example: python temp3.py 20230201 1 day")
        sys.exit(1)

    # Parse the bar size argument
    bar_size_parts = bar_size_string.split()
    print("bar_size_parts:",bar_size_parts)
    if len(bar_size_parts) != 2:
        print("Error: Invalid bar size format")
        sys.exit(1)

    numerical_value = int(bar_size_parts[0])
    time_unit = bar_size_parts[1]


    # Validate the bar size
    valid_bar_sizes = [
        "1 secs", "5 secs", "10 secs", "15 secs", "30 secs",
        "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins",
        "1 hour", "2 hours", "3 hours", "4 hours", "8 hours",
        "1 day", "1 week", "1 month"
    ]

    if bar_size_string not in valid_bar_sizes:
        print("Error: Wrong bar size")
        sys.exit(1)

    #The barsize string value is correct
    global_bar_size = bar_size_string  # Store the bar size in the global variable

    # Validate the date
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    if approximate_start_date >= current_date:
        print("Error: Wrong date (should be in the past)")
        sys.exit(1)

    global wrapper, client, historical_data_thread
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
        client.request_data(contract, pair, approximate_start_date, bar_size)

    data_folder = "./historical_data/forex"
    for currency_pair in majors:
        currency_pair_folder = os.path.join(data_folder, f"{currency_pair[1][:6]}{currency_pair[1][6:]}")
        wrapper.check_data_order(currency_pair_folder)


if __name__ == "__main__":
    main()
