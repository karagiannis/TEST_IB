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
import utility_functions


reqId = 0
bar_size_str = None
DEBUG = False
class MyWrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.contract_map = {}
        self.data_received = False
        self.line_number = 1  # Initialize line number to 1
 

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
        global bar_size_str
        print("HistoricalDataEnd. ReqId:", reqId)
        self.data_received = True
        # Since historicalDataEnd is called after all historical data is received, we can save the data to CSV here.
        utility_functions.save_data_to_csv(self.contract_map,reqId, bar_size_str )
        print("save_data_to_csv returned")



class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper  # Store the instance of MyWrapper
        self.first_request = True  # Initialize to True for the first request
        

    def historical_data_worker(self):
        """
        Worker function for running the client event loop in a separate thread.
        """
        if not self.wrapper.data_received:  # Use 'self.wrapper.data_received' to access the instance variable
            self.run()  # Process IB messages
            time.sleep(0.1)
            # Wait for the historical data thread to complete
        

    def request_data(self, contract, instrument, end_datetime, duration, bar_size):
        global reqId
        global bar_size_str
        bar_size_str = bar_size
        reqId += 1
        self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}

        formatted_end_datetime = end_datetime.strftime('%Y%m%d %H:%M:%S') + ' US/Eastern'
        duration_sec = utility_functions.get_duration_seconds_from_duration_str(duration)

        print(f"Requesting data for {instrument} "
            f"from {(end_datetime - datetime.timedelta(seconds=duration_sec)).strftime('%Y%m%d %H:%M:%S')} to "
            f"{formatted_end_datetime} with a bar size {bar_size}.")

        self.reqHistoricalData(reqId, contract, formatted_end_datetime, duration, bar_size,
                                "MIDPOINT", 1, 1, False, [])

        print("Finished requesting data.")



wrapper = MyWrapper()
client = MyClient(wrapper)
historical_data_thread = None


def main():

    global wrapper, client, historical_data_thread
    wrapper = MyWrapper()
    client = MyClient(wrapper)
    client.connect("127.0.0.1", 7497, clientId=0)
    # Check if connected
    print(f"Connecting to IB Gateway/TWS. Is connected: {client.isConnected()}")

    
    # Start the historical data worker thread
    historical_data_thread = threading.Thread(target=client.historical_data_worker)
    historical_data_thread.start()

    top_directory = "./historical_data"
    collection_need_of_update = utility_functions.generate_update_list(top_directory)
    #print("collection_need_of_update:",collection_need_of_update)

    #generate the request list with appropriate IB duration strings
    request_list = utility_functions.generate_request_list(collection_need_of_update)
    utility_functions.make_data_requests(request_list,client)

    
    client.disconnect()
    historical_data_thread.join()
    client.keyboardInterruptHard()

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
# contract_map = {
#     0: {
#         'contract': contract1,
#         'data': [bar1, bar2, ..., bar5]
#     },
#     1: {
#         'contract': contract1,
#         'data': [bar1, bar2, ..., bar5]
#     },
#     2: {
#         'contract': contract1,
#         'data': [bar1, bar2, ..., bar5]
#     }
# }

