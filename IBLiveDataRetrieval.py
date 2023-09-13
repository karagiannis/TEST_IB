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
import subprocess
from ib_console import interactive_console  # Import the interactive_console function from ib_console.py
from multiprocessing import Process, Pipe
import code
import sys
import pty
import tkinter as tk
import queue  # Import the queue module

#reqId = 0
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
                print("bar is appended in historicalData, reqId, self.contract_map[reqId]['data']:",reqId, self.contract_map[reqId]['data'])

    def historicalDataEnd(self, reqId, start: str, end: str):
        global bar_size_str
        print("HistoricalDataEnd.self.contract_map,reqId, bar_size_str :", self.contract_map,reqId, bar_size_str)
        self.data_received = True
        # Since historicalDataEnd is called after all historical data is received, we can save the data to CSV here.
        utility_functions.save_data_to_csv(self.contract_map,reqId, bar_size_str )
        print("save_data_to_csv returned")



class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper  # Store the instance of MyWrapper
        self.first_request = True  # Initialize to True for the first request
        self.data_update_queue = queue.Queue()  # Create a queue for data update requests
        self.lock = threading.Lock()  # Initialize a lock


def historical_data_worker(self):
        while not self.wrapper.data_received:
            try:
                top_directory = self.data_update_queue.get(timeout=1)
                with self.lock:  # Assuming you have a lock attribute in your client class
                    utility_functions.update_data(self, top_directory)
            except queue.Empty:
                pass
            except Exception as e:
                print("Error:", e)

            # Wait for the historical data thread to complete


    def request_data(self, contract, instrument, end_datetime, duration, bar_size):
        #global reqId
        global bar_size_str
        bar_size_str = bar_size
        reqId = len(self.wrapper.contract_map)
        self.wrapper.contract_map[reqId] = {'data': [], 'contract': contract}


        formatted_end_datetime = end_datetime.strftime('%Y%m%d %H:%M:%S') + ' US/Eastern'
        duration_sec = utility_functions.get_duration_seconds_from_duration_str(duration)

        print(f"Requesting data for {instrument} "
            f"from {(end_datetime - datetime.timedelta(seconds=duration_sec)).strftime('%Y%m%d %H:%M:%S')} to "
            f"{formatted_end_datetime} with a bar size {bar_size}.")

        self.reqHistoricalData(reqId, contract, formatted_end_datetime, duration, bar_size,
                                "MIDPOINT", 1, 1, False, [])

        print("Finished requesting data.")



# Create a lock
data_update_lock = threading.Lock()

# Function to update data safely
def update_data_safely(client, top_directory):
    with data_update_lock:
        utility_functions.update_data(client, top_directory)

def process_command(pipe, input_entry):
    user_input = input_entry.get()
    pipe.send(user_input)


# Function to handle user input and communication with the main program
def terminal_input(pipe, top_directory, output_text, client):
    while True:
        user_input = pipe.recv()
        output_text.insert(tk.END, f"You wrote: {user_input}\n")

        # Use the 'client' object here to perform actions related to the 'client'
        client.data_update_queue.put(top_directory)

        if user_input == "exit":
            break





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
    #utility_functions.update_data(client, top_directory)
    # Create a Pipe for communication between processes
    main_pipe, terminal_pipe = Pipe()

    # Create the main window
    root = tk.Tk()
    root.title("Program Input/Output")

    # Create an input entry field
    input_label = tk.Label(root, text="Enter a command:")
    input_label.pack()
    input_entry = tk.Entry(root)
    input_entry.pack()

    # Create a button to process the command
    #process_button = tk.Button(root, text="Process", command=lambda: process_command(terminal_pipe))
    process_button = tk.Button(root, text="Process", command=lambda: process_command(terminal_pipe, input_entry))

    process_button.pack()

    # Create a text area for output
    output_text = tk.Text(root)
    output_text.pack()

    # Start the terminal input process
    terminal_process = Process(target=terminal_input, args=(main_pipe, top_directory, output_text, client))

    terminal_process.start()

    # Start the GUI main loop
    root.mainloop()

    # Clean up when the GUI is closed
    terminal_process.join()

    client.disconnect()


    historical_data_thread.join()
    client.keyboardInterruptHard()

if __name__ == "__main__":
    main()