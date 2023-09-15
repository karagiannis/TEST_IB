import datetime
import time
import threading  # Import threading module for multithreading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import utility_functions
from ibapi.ticktype import TickType



reqId = 0
bar_size_str = None
DEBUG = False

# Define locks for shared resources
contract_map_lock = threading.Lock()
reqId_lock = threading.Lock()
bar_size_str_lock = threading.Lock()

class MyWrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.contract_map = {}
        self.data_received = False
        self.bid_price = None
        self.ask_price = None

    # Override the error method to handle errors
    def error(self, e):
        # Handle exceptions thrown within the API code
        print(f"API Exception: {e}")


    def error(self, str):
        # Handle errors as string messages
        print(f"API Error: {str}")


    def error(self, id, errorCode, errorMsg, advancedOrderRejectJson):
        # Handle TWS/Gateway errors
        print(f"TWS Error - ID: {id}, Code: {errorCode}, Message: {errorMsg}")
        if advancedOrderRejectJson:
            print(f"Advanced Order Reject JSON: {advancedOrderRejectJson}")


    def historicalData(self, reqId, bar):
        print("Received historical data:", bar)
        # Lock access to contract_map
        with contract_map_lock:
            # If reqId is -1, it means this is not a specific historical data request,
            # but rather a response related to the data farm connection
            if reqId == -1:
                print("Data farm connection is OK:", bar)

            # If reqId is not -1, it means this is a response to a specific historical data request
            # and you can handle the OHLC data accordingly
            else:
                if reqId in self.contract_map:
                    self.contract_map[reqId]['data'].append(bar)
                    print("bar is appended in historicalData, reqId, self.contract_map[reqId]['data']:", reqId,
                          self.contract_map[reqId]['data'])

    def historicalDataEnd(self, reqId, start: str, end: str):
        global bar_size_str
        print("HistoricalDataEnd. ReqId:", reqId)
        self.data_received = True
        # Lock access to contract_map and bar_size_str
        with contract_map_lock, bar_size_str_lock:
            # Since historicalDataEnd is called after all historical data is received, we can save the data to CSV here.
            utility_functions.save_data_to_csv(self.contract_map, reqId, bar_size_str)
            print("save_data_to_csv returned")

    def historicalDataUpdate(self, reqId, bar):
        print("Received historical data update:", bar)

        # Lock access to contract_map
        with contract_map_lock:
            if reqId in self.contract_map:
                self.contract_map[reqId]['data'].append(bar)
                print("Bar is appended in historicalDataUpdate, reqId, self.contract_map[reqId]['data']:", reqId,
                      self.contract_map[reqId]['data'])
                # Call save_data_to_csv here as well
                utility_functions.save_data_to_csv(self.contract_map, reqId, bar_size_str)
        print("save_data_to_csv returned, stored live data")

    def tickPrice(self, tickerId, field, price, attribs):
        # # Process the received tick price data
        # Use integer values for field (e.g., 1 for BID, 2 for ASK)
        if field == 1:  # BID
            self.bid_price = price
        elif field == 2:  # ASK
            self.ask_price = price

        # Check if both bid and ask prices are available
        if self.bid_price is not None and self.ask_price is not None:
            print(f"Bid Price: {self.bid_price}, Ask Price: {self.ask_price}")


class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper  # Store the instance of MyWrapper

    def historical_data_worker(self):
        """
        Worker function for running the client event loop in a separate thread.
        """
        if not self.wrapper.data_received:  # Use 'self.wrapper.data_received' to access the instance variable
            self.run()  # Process IB messages
            time.sleep(0.1)
            # Wait for the historical data thread to complete

    def request_data(self, contract, instrument, end_datetime, duration, bar_size, keepUpToDate=False):
        global reqId
        global bar_size_str
        # Lock access to reqId and bar_size_str
        with reqId_lock, bar_size_str_lock, contract_map_lock:
            bar_size_str = bar_size
            reqId += 1
            self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}

            print(f"Requesting data for {instrument} "
                  f"with a bar size {bar_size}.")

            formatted_end_datetime = end_datetime.strftime('%Y%m%d %H:%M:%S') + ' US/Eastern'

            if keepUpToDate:
                self.reqHistoricalData(reqId, contract, "", duration, bar_size,
                                       "MIDPOINT", 1, 1, keepUpToDate, [])
            else:
                self.reqHistoricalData(reqId, contract, formatted_end_datetime, duration, bar_size,
                                       "MIDPOINT", 1, 1, keepUpToDate, [])

            print("Finished requesting data.")

    def request_live_stream_data(self, contract, instrument, bar_size):
        global reqId
        global bar_size_str
        # Lock access to reqId and bar_size_str
        with reqId_lock, bar_size_str_lock, contract_map_lock:
            bar_size_str = bar_size
            reqId += 1
            self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}

            print(f"Requesting data for {instrument} with a bar size {bar_size}.")
            tickerId = reqId
            genericTickList = ""
            self.reqMktData(tickerId, contract, genericTickList, False, False,
                                [])  # Set snapshot to False for streaming data

            print("Requested live streaming data.")

            print("Finished requesting live streaming data.")


wrapper = MyWrapper()
client = MyClient(wrapper)
historical_data_thread = None
user_input_thread = None


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

    # Start the user input  thread
    # user_input_thread = threading.Thread(target=utility_functions.create_forex_data_request_gui(client,utility_functions.request_live_data_for_pair))
    # user_input_thread.start()

    # Start the user input thread with request_live_data_for_pair_snd
    user_input_thread = threading.Thread(target=utility_functions.create_forex_data_request_gui(client,utility_functions.request_live_data_for_pair_snd))
    user_input_thread.start()

    #top_directory = "./historical_data"
    

    client.disconnect()
    historical_data_thread.join()
    user_input_thread.join()
    client.keyboardInterruptHard()


if __name__ == "__main__":
    main()