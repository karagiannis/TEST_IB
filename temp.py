
'''
                +----------------------+
                |     EWrapper         |
                +----------------------+
                |  contract_map        |
                |  data_received       |
                |  line_number         |
                +----------------------+
                     ^
                     |
                     |
+------------------+ |
| MyWrapper         | |
+------------------+ |
|  __init__()       | |
|  historicalData() | | Inherits
|  historicalDataEnd() | |
+------------------+ |
                     |
                     |    +----------------------+
                     |    |      EClient         |
                     |    +----------------------+
                     |    |                      |
                     |    |                      |
                     |    |                      |
                     |    |                      |
                     |    +----------------------+
                     |              ^
                     |              |
+------------------+ |              |
| MyClient         | |              |
+------------------+ |              |
|  __init__()      | |              |
|  request_data()  | |  Inherits    |
|  historical_data_worker()  <---  |
+------------------+              |
                                  |
                                  |
                                  |
                                  |
                             +------------------+
                             | Contract         |
                             +------------------+
                             | symbol           |
                             | currency         |
                             | exchange         |
                             | secType          |
                             +------------------+




MyWrapper class:
The MyWrapper class is a custom implementation of the EWrapper class provided by the Interactive Brokers API.
 It is used to handle events and responses received from the TWS (Trader Workstation) or IB Gateway.

In the __init__ method of MyWrapper, we call the __init__ method of the superclass EWrapper using super().__init__()
 This is done to ensure that the initialization of the EWrapper class is properly executed before we add our
 custom attributes and methods to the MyWrapper class.

The MyWrapper class has a few attributes, such as contract_map, data_received, and line_number.
These attributes are used to keep track of data and events received from the TWS.

MyClient class:
The MyClient class is a custom implementation of the EClient class provided by the Interactive Brokers API.
 It is used to interact with the TWS or IB Gateway and make requests for historical data.

In the __init__ method of MyClient, we call the __init__ method of the superclass
EClient using EClient.__init__(self, wrapper).
This ensures that the EClient class is properly initialized and that we can use its methods for making requests.

Additionally, MyClient has an attribute self.wrapper, which is initialized with the wrapper passed
 as an argument to the __init__ method. By storing the instance of MyWrapper in self.wrapper,
we can access the methods and attributes of the MyWrapper class from within MyClient.

wrapper = MyWrapper() and client = MyClient(wrapper):
In the main part of the code, we create instances of MyWrapper and MyClient classes.
 We create wrapper as an instance of MyWrapper, and then we pass this instance to the MyClient constructor
  as an argument when creating client.

This allows client to have access to the MyWrapper instance stored in self.wrapper.
 It means that client can interact with and call methods defined in MyWrapper, and vice versa.

The reason for this design is to separate the handling of events and responses (MyWrapper)
 from the interaction with the TWS and making historical data requests (MyClient).
  By having separate classes, it provides better modularity and organization of code.

  Yes, the wrapper can call methods in the client. The reason for this is that the MyWrapper class is a subclass
  of EWrapper, which is the wrapper class provided by the Interactive Brokers API. The EWrapper class defines
   various callback methods that the client (an instance of MyClient, which is a subclass of EClient)
    can call when certain events or data are received from the TWS/IB Gateway.

Inside the MyWrapper class, you can override these callback methods to implement custom behavior when specific events
occur. For example, in the code provided earlier, MyWrapper overrides the historicalData and historicalDataEnd methods
 to handle historical data and the end of historical data respectively.

Now, the MyClient class (subclass of EClient) uses an instance of MyWrapper as self.wrapper to handle those callbacks.
 So, when certain events occur or data is received from TWS/IB Gateway, the MyWrapper instance can call methods
  defined in MyClient (if needed) to further process or handle the received data.
   This two-way communication allows the wrapper and client to interact and work together to manage the connection
    and data retrieval from TWS/IB Gateway.

In summary, while wrapper can call methods in client, it is more common for the client to call methods in wrapper,
 as the primary purpose of the wrapper is to handle events and data received from TWS/IB Gateway.
  The client acts as the main control and communication point with TWS/IB Gateway,
  while the wrapper provides the callback methods to handle specific events and data as needed.

In the provided code, the MyWrapper class can call methods in the MyClient class through the self.wrapper attribute.
My own explanation: Because wrapper is inside of client and sees therefore all methods and members of client

Here's an example of how it's done:

    In the MyClient class, the wrapper instance is passed as an argument to the constructor (__init__ method)
     and stored as self.wrapper:

python

class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper  # Store the instance of MyWrapper

    Inside the MyWrapper class, there is a method called save_data_to_csv, which saves historical data to a CSV file.
     This method accesses the MyClient instance through self.wrapper and calls its method
      reqHistoricalData to request historical data:



python

class MyWrapper(EWrapper):
    def save_data_to_csv(self, reqId, filename):
        # ...
        self.reqHistoricalData(reqId, contract, query_time, duration_str, "1 day", "MIDPOINT", 1, 1, False, [])

In this example, MyWrapper can call the reqHistoricalData method of MyClient through self.wrapper.
 This demonstrates that the wrapper can indeed call methods in the client if needed.







The interaction between MyClient and MyWrapper is essential for handling callbacks and events received from the
 TWS/IB Gateway. The MyClient manages the connection to TWS/IB Gateway and the execution of API requests,
  while the MyWrapper handles the received data and events by implementing callback methods provided by the IB API.
   This interaction allows for seamless communication and coordination between the two classes.


   In Python, you are free to declare and use instance attributes (like self.xxx) as needed for your classes.
   It is not limited to attributes present in the base class (EClient) or any other class. When you define a class,
    you can add any instance attributes you need to store data within the instance of that class.

In the code you provided:

python

class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper  # Store the instance of MyWrapper

Here, MyClient is a subclass of EClient. In the __init__ method, it first calls the __init__ method
of the parent class EClient using EClient.__init__(self, wrapper).
This ensures that the initialization of the parent class is done properly before initializing
 the specific attributes of MyClient.

After that, it sets the instance attribute self.wrapper to the value of the wrapper argument passed to the constructor.
 This attribute is specific to MyClient and not present in the parent class EClient.
 This allows MyClient instances to store a reference to the MyWrapper instance passed during initialization.

Remember that Python is a dynamic language, and you can freely add, modify,
 or remove instance attributes during runtime.
 The only requirement is that you should properly initialize and
  define the attributes you need for the proper functioning of your class.



Apologies for the confusion. Let me provide you with a detailed explanation of the code and the functionality
of the classes.


MyWrapper Class:
    This class inherits from the EWrapper class provided by the Interactive Brokers API.
     It acts as a wrapper around the API to handle callbacks and events related to the data received from TWS
      (Trader Workstation) or IB Gateway.
    The class constructor (__init__) initializes the instance variables:
        contract_map: A dictionary to store contract data and related requests.
        data_received: A flag to indicate if historical data has been received.
        line_number: A counter to track the line number for printing data.
    The class has two callback methods:
        historicalData: This method is called when historical data is received from TWS.
         It handles both generic responses (like data farm connection status) and specific historical data requests.
        historicalDataEnd: This method is called when the historical data request is completed.

MyClient Class:
    This class inherits from the EClient class provided by the Interactive Brokers API.
    It is responsible for managing the client-side connection to TWS or IB Gateway.
    The class constructor (__init__) takes the wrapper parameter and stores it as an instance variable self.wrapper.
    The class has two important methods:
        historical_data_worker: This method is a worker function that runs in a separate thread.
         It continuously processes incoming messages from TWS using the self.run() method until
         data_received becomes True.
        request_data: This method is used to request historical data for a specific contract.
        It calculates the start and end dates based on the approx_start_date provided and makes a request
         to TWS using self.reqHistoricalData.
        Inside request_data, it also starts the historical_data_worker thread to handle incoming messages.

Main Function:
    The main() function is responsible for the main execution flow of the program.
    It starts by creating instances of MyWrapper and MyClient as wrapper and client, respectively.
    It then establishes a connection to TWS or IB Gateway using client.connect.
    Next, it defines a list of major currency pairs (majors) and initiates the historical_data_worker thread.
    For each major currency pair, it creates a contract and calls client.request_data to request historical data.
    After requesting historical data for all pairs, it enters a loop where it processes incoming messages using
    client.run() until wrapper.data_received becomes True.
    Finally, it waits for the historical_data_worker thread to complete using historical_data_thread.join().

Explanation of ASCII UML Diagram:

    The diagram shows three classes: EWrapper, EClient, and Contract.
    MyWrapper and MyClient are custom classes that inherit from EWrapper and EClient, respectively.
    Contract is a class from the Interactive Brokers API and is used to represent financial instruments
    like currency pairs.
    The diagram shows the inheritance relationships and the direction of data flow between classes.


'''
# Explanation of MyClient and MyWrapper Classes:

# In the code provided, we have two classes: MyClient and MyWrapper. MyClient is a subclass of EClient, and MyWrapper
# is a subclass of EWrapper. Let's break down the code and explain how these classes work together.

# MyWrapper Class:
# The MyWrapper class is a subclass of EWrapper, which is a class provided by the Interactive Brokers API (IB API).
# EWrapper is essentially a callback interface that allows you to receive various messages from the IB Gateway/TWS.
# By creating a subclass of EWrapper, we can override its methods to customize how we handle these messages.

# In the __init__ method of MyWrapper, we initialize some instance attributes:
#   - contract_map: A dictionary that maps request IDs to contract objects. It will store information about contracts
#     for which we are requesting historical data.
#   - data_received: A boolean flag to keep track of whether historical data has been received or not.
#   - line_number: An integer that starts at 1 and is used to display the line number when printing historical data.

# The historicalData method is an overridden method from EWrapper. It is called when historical data is received from
# the TWS. Inside this method, we check if the reqId (request ID) is -1, which indicates that this is not specific
# historical data but a response related to the data farm connection. If the reqId is not -1, it means it is a response
# to a specific historical data request, and we can handle the OHLC (Open, High, Low, Close) data accordingly.

# The historicalDataEnd method is called when the historical data request is completed. We update the data_received
# flag to True when this method is called.

# The save_data_to_csv method is a custom method we added. It is used to save historical data to a CSV file for each
# contract requested. We append the OHLC data to the corresponding CSV file.

# MyClient Class:
# The MyClient class is a subclass of EClient, which is also provided by the IB API. EClient is responsible for
# sending requests to the TWS and handling other client-side interactions with the IB Gateway/TWS.

# In the __init__ method of MyClient, we call the __init__ method of the parent class EClient using
# EClient.__init__(self, wrapper). This ensures that the initialization of the parent class is done properly before
# initializing the specific attributes of MyClient.

# We store the instance of MyWrapper (wrapper) as an instance attribute self.wrapper. This allows client to have access
# to the MyWrapper instance stored in self.wrapper. It means that client can interact with and call methods defined in
# MyWrapper, and vice versa.

# The request_data method in MyClient is used to request historical data from TWS for a given contract. We calculate the
# start date and end date for the historical data based on an approximate start date. Then, we use the reqHistoricalData
# method from EClient to send the request for historical data to the TWS.

# The historical_data_worker method is a separate worker function that runs the client event loop in a separate thread.
# This is done to avoid blocking the main thread and to allow handling messages from the TWS concurrently.

# In the main function, we create instances of MyClient and MyWrapper, and we establish a connection to the TWS.
# We define a list of major currency pairs (majors) and start a separate thread for each currency pair to request
# historical data concurrently. The historical data is then saved to CSV files using the save_data_to_csv method of
# MyWrapper.

# The main function then waits for all historical data requests to be completed using the data_received flag.
# The historical data thread is joined back to the main thread once all data is received.

# Summary:
# The MyClient class handles client-side interactions with the TWS and sends requests for historical data.
# The MyWrapper class handles incoming messages from the TWS and processes historical data as it arrives.
# By having the MyClient instance store a reference to the MyWrapper instance, we can establish a bidirectional
# communication channel between the two classes, allowing them to interact and share data as needed.







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


def find_first_friday(approximate_start_date):
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

    def request_data(self, contract, pair, approx_start_date):
        eastern_tz = pytz.timezone('US/Eastern')

        # Find the first Saturday in January 2005
        start_date = find_first_friday(approx_start_date)

        # Calculate the end date as the next Friday (6 days after Saturday)
        end_date = start_date + datetime.timedelta(days=6)

        # Set the endDateTime to the end of the day on the next Friday
        end_date_time = end_date.replace(hour=23, minute=59, second=59).strftime("%Y%m%d %H:%M:%S") + ' US/Eastern'

        reqId = len(self.wrapper.contract_map) + 1
        self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}
        print(
            f"Requesting data for {contract.symbol} {contract.currency} from {start_date.strftime('%Y%m%d %H:%M:%S')} to {end_date_time} with a bar size of 1 day.")
        self.reqHistoricalData(reqId, contract, end_date_time, "5 D", "1 day", "MIDPOINT", 1, 1, False, [])
        time.sleep(3)  # Wait for 3 seconds before making another request

        # Save data to CSV
        self.wrapper.save_data_to_csv(reqId, pair)

        print("Finished requesting data.")
        self.wrapper.data_received = True  # Update the flag when all data has been requested.


wrapper = MyWrapper()
client = MyClient(wrapper)


def main():
    approximate_start_date="20230201"
    global wrapper, client
    # wrapper = MyWrapper()
    # client = MyClient(wrapper)
    client.connect("127.0.0.1", 7497, clientId=0)
    # Check if connected
    print(f"Connecting to IB Gateway/TWS. Is connected: {client.isConnected()}")

    # Define major currency pairs
    majors = [
        (Contract(), "EURUSD")
        # (Contract(), "GBPUSD"),
        # (Contract(), "USDJPY"),
        # (Contract(), "USDCHF"),
        # (Contract(), 'AUDUSD'),
        # (Contract(), "USDCAD"),
        # (Contract(), "NZDUSD"),
        # (Contract(), "EURGBP")
    ]
    # Start the historical data worker thread
    historical_data_thread = threading.Thread(target=client.historical_data_worker)
    historical_data_thread.start()

    for contract, pair in majors:
        contract.symbol = pair[:3]
        contract.currency = pair[3:]
        contract.exchange = "IDEALPRO"
        contract.secType = "CASH"  # Specify the security type
        client.request_data(contract, pair, approximate_start_date)

    while not wrapper.data_received:
        client.run()  # Process IB messages
        time.sleep(0.1)

        # Wait for the historical data thread to complete
    historical_data_thread.join()


if __name__ == "__main__":
    main()
