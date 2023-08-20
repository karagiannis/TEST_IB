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
from barsizes import valid_bar_sizes, bar_size_suffix
from ContractSamples import ContractSamples

global_bar_size = None  # Initialize the global_bar_size variable
DEBUG = False
class MyWrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.contract_map = {}
        self.data_received = False
        self.line_number = 1  # Initialize line number to 1

    def save_data_to_csv(self, reqId, filename, bar_size):
        if DEBUG:
            print("Inside save_data_to_csv")
        # Specify the data folder where CSV files will be stored
        data_folder = "./historical_data/forex"

        # Create the data folder if it doesn't exist
        os.makedirs(data_folder, exist_ok=True)

        # Get the contract associated with the request ID
        contract = self.contract_map[reqId]['contract']

        # Create a unique currency pair identifier using the contract symbol and currency
        currency_pair = f"{contract.symbol}{contract.currency}"

        bar_size_folder_name = bar_size.replace(" ", "_")
        bar_size_filename = bar_size.replace(" ", "_")

        # Create the folder path for the specific currency pair
        currency_pair_folder = os.path.join(data_folder, f"{currency_pair}/{ bar_size_folder_name}")

        # Create the currency pair folder if it doesn't exist
        os.makedirs(currency_pair_folder, exist_ok=True)
        # Replace space with underscore in bar_size for folder and filename


        # Check if the specified bar size exists in the mapping
        if bar_size in valid_bar_sizes:
            # Add the bar size suffix to the filename
            filename_with_suffix = f"{filename}_{bar_size_filename}.csv"

        else:
            # If the bar size is not found, use a default suffix
            filename_with_suffix = f"{filename}_default.csv"

        # Create the complete file path for the CSV file
        file_path = os.path.join(currency_pair_folder, filename_with_suffix)


        # Check if the file already exists
        if os.path.exists(file_path):
            try:
                # Load existing data from the CSV file
                existing_data = pd.read_csv(file_path)

                # Convert the 'Date' column in existing_data to datetime objects
                existing_data['Date'] = pd.to_datetime(existing_data['Date'])

                # Create a DataFrame with new data to append
                new_data = pd.DataFrame([
                    [pd.to_datetime(data_point.date), data_point.open, data_point.high, data_point.low,
                     data_point.close, data_point.volume]
                    for data_point in self.contract_map[reqId]['data']
                ], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

                # Concatenate the existing data and new data
                combined_data = pd.concat([existing_data, new_data])

                # Remove duplicate rows based on the 'Date' column
                combined_data.drop_duplicates(subset=['Date'], keep='first', inplace=True)

                # Sort the combined data by the 'Date' column
                combined_data.sort_values(by='Date', inplace=True)

                # Write the combined data to the CSV file
                combined_data.to_csv(file_path, index=False)

            except Exception as e:
                print(f"Error occurred while appending data to existing CSV file: {str(e)}")
        else:
            if DEBUG:
                print("Inside save_data_to_csv, trying to create csv-file because none existed")
            try:
                # Create a new file and write the data
                with open(file_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    for data_point in self.contract_map[reqId]['data']:
                        writer.writerow([data_point.date, data_point.open, data_point.high, data_point.low,
                                         data_point.close, data_point.volume])
            except Exception as e:
                print(f"Error occurred while creating and writing data to the CSV file: {str(e)}")

    def check_data_order(self, currency_pair_folder):
        # List all files in the specified currency pair folder
        data_files = os.listdir(currency_pair_folder)

        # Extract the date parts from the file names and sort them
        dates = [int(file_name.split('_')[1].split('.')[0]) for file_name in data_files]
        dates.sort()

        # Iterate through the sorted dates to check for missing data
        for i in range(1, len(dates)):
            # Get the previous and current dates in the sorted list
            prev_date = dates[i - 1]
            curr_date = dates[i]

            # Check if there are missing dates between the previous and current dates
            if curr_date - prev_date > 1:
                # Print a message indicating the range of missing dates
                print(f"You are missing data for dates {prev_date + 1} to {curr_date - 1}")

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
        print("save_data_to_csv returned")


    def find_first_friday(self, approximate_start_date):
        # Find the first friday from an approximate given start date
        start_date = datetime.datetime.strptime(approximate_start_date, "%Y%m%d")  # Convert to datetime object
        while start_date.weekday() != 4:  # 4 represents Friday (Monday is 0, Sunday is 6)
            start_date += datetime.timedelta(days=1)
        return start_date

    def headTimestamp(self, reqId: int, headTimestamp: str):
        print("HeadTimestamp. ReqId:", reqId, "HeadTimeStamp:", headTimestamp)


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

    return bar_size in valid_bar_sizes



def main():
    global global_bar_size

    '''
    approximate_start_date = None
    bar_size = None

    if len(sys.argv) == 4:
        approximate_start_date = sys.argv[1]
        bar_size_string = sys.argv[2] + " " + sys.argv[3]

    else:
        print("Usage: python temp3.py <approximate_start_date> <bar_size>")
        print("Example: python temp3.py 20230201 1 day")
        sys.exit(1)
    '''
    approximate_start_date = "20230215"
    bar_size_string = "1 day"

    # Parse the bar size argument
    bar_size_parts = bar_size_string.split()
    print("bar_size_parts:",bar_size_parts)
    if len(bar_size_parts) != 2:
        print("Error: Invalid bar size format")
        sys.exit(1)

    numerical_value = int(bar_size_parts[0])
    time_unit = bar_size_parts[1]



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
        #contract = ContractSamples.USStockAtSmart()
        #client.reqHeadTimeStamp(4101, contract, "TRADES", 0, 1)
        # Wait for a moment to allow the response to come in
        #time.sleep(10)
        client.request_data(contract, pair, approximate_start_date, global_bar_size)
        #client.reqHistoricalData(4102, contract, " 20230213 00:00:00 US/Eastern", "5 D", "1 day",
        #                  "MIDPOINT", 1, 1, False, [])
        time.sleep(3)  # Wait for 3 seconds before making another request

    #data_folder = "./historical_data/forex"
    #for contract, currency_pair in majors:
    #    currency_pair_folder = os.path.join(data_folder, f"{currency_pair[3:]}{currency_pair[:3]}")
    #    wrapper.check_data_order(currency_pair_folder)
    client.disconnect()


if __name__ == "__main__":
    main()

#Historical Data Update Program:

    #This program should have the logic to determine which instruments' historical data needs updating.
    #It should analyze the existing historical data to identify the last date present.
    #For each instrument, calculate the duration since the last available date and the current date (Saturday).
    #Loop through the allowed bar sizes and request historical data for the missing period.
    #for all folders in ./historical_data: /stock or forex folders
    #   for all subfolders in folders:/ could be IBM in stock folder or EURUSD folder in forex folder
    #       for all subsubfolders in subfolders: /Could be 1 minute -folder or 15-minute folder or daily -folder
    #               open the file
    #               save the pathway
    #               Find out the bar_size from the file name
    #               if you can  read the date, hour and minute of the last stored OHLC bar
        #               if the date and hour and minute is not the same for the last bar on yesterday
        #               (which is different if we are updating daily bars, hourly bars or minute bars)
        #               find out how many bars that can be requested and calculate the corresponding duration period
        #               find out how many request that can be done within the 15 minute time period
        #               Do the requests and keep track of how many bars that have been recieved
        #               when  limit is reached (60 request within 10 minutes) then paus so that you have
        #               done 60 requests within 11 minutes.
        #               Reset the counter and reset the timer
#                   else the data is lost and needs to be requested 6 months back in time
#                       with consideration to number of request per 10 minutes
