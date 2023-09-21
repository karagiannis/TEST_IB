import os
import pandas as pd
from ibapi.contract import Contract
from datetime import datetime, timedelta
import pytz
import csv
from allowable_duration import allowable_duration
from allowable_bar_sizes import allowable_bar_sizes
import logging
import time
import re
from ibapi.client import EClient
from allowable_forex_pairs import allowable_forex_pairs
from allowable_US_Stocks import allowable_US_Stocks
from allowable_US_Bonds import allowable_US_Bonds
from dateutil.parser import parse
from settings import *
import pdb
import tkinter as tk
from tkinter import Entry



# Rest of your code



logging.basicConfig(filename='app.log', level=logging.DEBUG)


def generate_update_list(top_directory: str):
    logging.debug("inside generate_update_list function, fn_1")
    collection_csv_file_paths = listAllCsvfilesPaths(top_directory)
    logging.debug(f"Found {len(collection_csv_file_paths)} CSV files")
    collection_need_of_update = []
    six_months_ago = datetime_six_months_ago_NewYork_time()
    last_friday = get_last_friday_date()
    now = datetime_now_NewYork_time()

    for csv_file_path in collection_csv_file_paths:
        logging.debug(f"Processing CSV file: {csv_file_path}")
        bar_size = bar_size_from_file_path(csv_file_path)
        allowable_duration_in_bar_request = maximum_duration_time_when_requesting_bars(bar_size)
        instrument = get_instrument_from_file_name(csv_file_path)
        contract = get_contract_from_csv_file_path(csv_file_path)

        if file_is_empty(csv_file_path):
            start_datetime = six_months_ago
        else:
            last_saved_bar_datetime_str = last_OHLC_date_from_csv_file(csv_file_path)
            last_saved_bar_datetime_str=transform_datetime_format(last_saved_bar_datetime_str)

            # Use pandas with 'mixed' format for datetime parsing
            last_saved_bar_datetime = pd.to_datetime(last_saved_bar_datetime_str, format='mixed')
            eastern_tz = pytz.timezone('US/Eastern')
            last_saved_bar_datetime = eastern_tz.localize(last_saved_bar_datetime)


            start_datetime = last_saved_bar_datetime
            end_datetime = next_datetime_for_bar_request(last_saved_bar_datetime, bar_size)

        if todays_week_day() == 5 or todays_week_day() == 6:
            end_datetime = last_friday
        else:
            end_datetime = now

        actual_duration = (end_datetime-start_datetime).total_seconds()
        logging.debug(f"Calculated actual_duration: {actual_duration}")
        update_dict = {
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'actual_duration':actual_duration,
            # "max_allowable_duration_look_back": get_duration_seconds_from_duration_str(\
            #     maximum_duration_time_when_requesting_bars(bar_size)),
            'contract': contract,
            'instrument': instrument,
            'bar_size': bar_size#,
            # 'max_duration': allowable_duration_in_bar_request,
            # 'file_path': csv_file_path
        }
        logging.debug(f"Generated update_dict: {update_dict}")

        collection_need_of_update.append(update_dict)
    logging.debug("Completed generate_update_list function")
    return collection_need_of_update

def listAllCsvfilesPaths(top_directory):
    logging.debug("Inside listAllCsvfilesPaths, fn_2")
    csv_filepath_collection = []  # Create an empty list to store file paths
    try:
        # Walk through the directory tree starting from 'top_directory'
        for root, dirs, sub_dirs in os.walk(top_directory):
            try:
                for file in sub_dirs:
                    # Check if the file has a .csv extension
                    if file.endswith('.csv'):
                        # Construct the full file path and add it to the list
                        csv_filepath_collection.append(os.path.join(root, file))
            except Exception as e:
                logging.error(f"Error in listAllCsvfilesPaths while traversing '{root}'/'{dirs}': {e}")
    except OSError as e:
        logging.error(f"Error in listAllCsvfilesPaths while traversing '{top_directory}': {e}")

    # Return the list of CSV file paths
    return csv_filepath_collection

# we will return the date for the friday 6 months ago
# with the hour of the end of the trading day
def datetime_six_months_ago_NewYork_time():
    logging.debug("Inside datetime_six_months_ago_NewYork_time(), fn_3 ")
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    six_months_ago = current_time - timedelta(days=6 * 30)
    while six_months_ago.weekday() != 4:
        six_months_ago = six_months_ago + timedelta(days=1)
        six_months_ago = six_months_ago.replace(hour=23, minute=59, second=59)  # replace works on the datetime object!
    return six_months_ago  # 

def datetime_now_NewYork_time():
    logging.debug("datetime_now_NewYork_time, fn_4")
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    return current_time


def bar_size_from_file_path(file_path):
    logging.debug("bar_size_from_file_path, fn_5")
    filename = os.path.basename(file_path)
    match = re.search(r'_(.+?)\.csv', filename)
    if match:
        bar_size = match.group(1).replace('_', ' ')
        #print("bar_size:", bar_size)
        return bar_size
    return None

# How many bars is it allowed to grab for a certain bar_size translates
# to the duration parameters. For instance for daily bars we are allowed
# to set duration  to 1 year which is coded "1 Y"
def maximum_duration_time_when_requesting_bars(bar_size: str):
    logging.debug("Inside maximum_duration_time_when_requesting_bars, fn_6")
    #print("Inside maximum_duration_time_when_requesting_bars, bar_size:", bar_size)
    if bar_size in allowable_bar_sizes:
        bar_size_seconds = allowable_bar_sizes[bar_size]
        largest_duration = None
        for duration, range_list in allowable_duration.items():
            if range_list[0][0] <= bar_size_seconds:
                largest_duration = duration
        return largest_duration
    else:
        raise ValueError("Inside maximum_duration_time_when_requesting bars\
                          - bar_size not in allowable bar_sizes")



# File name is for instance ./historical_data/forex/USDCAD/1_day/USDCAD_1_day.csv
# We split at '/' then take the last part and then split again and take the zeroth part
def get_instrument_from_file_name(file_path):
    logging.debug("Inside get_instrument_from_file_name, fn_7")
    # Extracting the base filename using os.path.basename
    filename = os.path.basename(file_path)
    # Splitting the filename by underscore and taking the first part
    instrument = filename.split('_')[0]
    return instrument  # Example: "EURUSD", "IBM"


# When we update our data collection of csv-file of bar data
 # we need to request new bar data for a specific contract say EURUSD
# specifying the correct exchange IDEALPRO or SMART and indicate if it is
# what kind of instrument class it is. Here we assume that in addition to our forex folder
# ./historical_data/forex/USDCAD/1_day/USDCAD_1_day.csv
# ./historical_data/forex/NZDUSD/1_day/NZDUSD_1_day.csv
# we also have
# ./historical_data/USStock/....
# ./historical_data/USBond/....
# with a similar structure
def get_contract_from_csv_file_path(file_path):
    logging.debug("Inside get_contract_from_csv_file_path, fn_8")
    instrument_class = get_instrument_class_from_file_path(file_path)
    contract = Contract()
    if instrument_class == 'forex':
        instrument = get_instrument_from_file_name(file_path)  # Example: 'EURUSD'
        contract.symbol = instrument[:3]
        contract.currency = instrument[3:]
        contract.exchange = "IDEALPRO"
        contract.secType = "CASH"  # Specify the security type
    elif instrument_class == 'US_Stock':
        contract.symbol = get_instrument_from_file_name(file_path)  # Example: 'IBM'
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"
        contract.primaryExchange = "ARCA"
    elif instrument_class == 'US_Bond':
        contract.symbol = get_instrument_from_file_name(file_path)  # Example CUSP_id "912828C57"
        contract.secType = "BOND"
        contract.exchange = "SMART"
        contract.currency = "USD"

    return contract


# We only have the folder "forex" but suppose
# we also have ./historical_data/USStock/....
# We need to know the type of the instrument because it determines which
# exchange we are going to enter in Contract
# Contract.exchange = "IDEALPRO" for Forex or
# Contract.exchage = "SMART" for US Stock and ETFs
def get_instrument_class_from_file_path(file_path):
    logging.debug("get_instrument_class_from_file_path, fn_9")
    instrument_class = file_path.split('/')[2]
    return instrument_class  # Example "forex" or "USStock" or "USBond"



def file_is_empty(file_path):
    logging.debug("Inside file_is_empty, fn_10")
    try:
        return os.path.getsize(file_path) == 0
    except OSError as e:
        logging.error(f"Error in file_is_empty while checking file size for '{file_path}': {e}")
        return False


def last_OHLC_date_from_csv_file(file_path):
    logging.debug("Inside last_OHLC_date_from_csv_file, fn_11")
    try:
        with open(file_path, "rb") as file:
            try:
                # Move the file pointer to the second-to-last character
                # from the end of the file (backward seeking)
                file.seek(-2, os.SEEK_END)

                # While reading characters in reverse...
                while file.read(1) != b'\n':
                    # Backtrack the file pointer by 2 bytes (1 character)
                    file.seek(-2, os.SEEK_CUR)
            except OSError:
                # If there's an exception, reset the file pointer to the beginning
                file.seek(0)

            # Read the last line as a string
            last_line = file.readline().decode()
    except Exception as e:  # Add an except block here
        # Handle the exception if needed
        print(f"Error occurred: {str(e)}")

    # Split the last line by comma and extract the first part (date and time)
    timedate = last_line.split(',')[0]

    # Return the extracted date and time
    return timedate  # "2023-08-10 00:00:00.000000000"



# Here we assume that we have made a maximum duration request for a particular
# bar_size and we must take into account the max duration to calculate a
# new end_date_time because the IBAPI does only allow you to specify a
# an end_date_time and a duration which is backwards in time.
# end_date_time = duration + start_time, where start_time is
# the end_time of the previous request
def next_datetime_for_bar_request(end_datetime, bar_size):
    logging.debug("Inside next_datetime_for_bar_request, fn_12")
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    duration = maximum_duration_time_when_requesting_bars(bar_size)
    if duration == "1 Y":
        duration = timedelta(days=365)
    elif duration == "1 M":
        duration = timedelta(days=30)
    elif duration == "1 W":
        duration = timedelta(weeks=1)
    elif duration == "2 D":
        duration = timedelta(days=2)
    elif duration == "1 D":
        duration = timedelta(days=1)
    elif duration == "28800 S":
        duration = timedelta(seconds=28800)
    elif duration == "14400 S":
        duration = timedelta(seconds=14400)
    elif duration == "3600 S":
        duration = timedelta(seconds=3600)
    elif duration ==  "1800 S":
        duration = timedelta(seconds=1800)
    elif duration == "120 S":
        duration = timedelta(seconds=120)
    elif duration ==  "60 S":
        duration = timedelta(seconds=60)
    else:
        raise ValueError(f"Invalid duration: {duration}")

    new_end_time = end_datetime + duration
    # Localize new_end_time with the same timezone as current_time
    #new_end_time = eastern_tz.localize(new_end_time)
    
    if new_end_time > current_time:
        return current_time
    else:
        return new_end_time  # Example: "20230223 15:15:00 US/Eastern" to be used in request_date with a duration (backwards)



def todays_week_day():
    logging.debug("Inside todays_week_day, fn_13")
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    week_day = current_time.weekday()
    return week_day  # Example 0 = Monday, 6=Friday

def get_duration_seconds_from_duration_str(duration_str):
    logging.debug("inside get_duration_seconds_from_duration_str, fn_14")
    if duration_str == "60 S":
        return 60
    elif duration_str == "120 S":
        return 120
    elif duration_str == "1800 S":
        return 1800
    elif duration_str == "3600 S":
        return 3600
    elif duration_str == "14400 S":
        return 14400
    elif duration_str == "28800 S":
        return 28800  
    elif duration_str == "1 D":
        return 24 * 60 * 60
    elif duration_str == "2 D":
        return 2 * 24 * 60 * 60
    elif duration_str == "1 W":
        return 7 * 24 * 60 * 60
    elif duration_str == "1 M":
        return 30 * 24 * 60 * 60
    elif duration_str == "1 Y":
        return 365 * 24 * 60 * 60
    

def generate_request_list(collection_list):
    logging.debug("Inside generate_request_list, fn_15")
    request_list = []
    for record in collection_list:
        chunks = divide_time_range(record['start_datetime'], record['end_datetime'],  record['bar_size'])
        for chunk in chunks:
            dict = {
                'end_datetime': chunk["chunk_end"],
                'contract': record['contract'],
                'instrument': record['instrument'],
                'bar_size': record['bar_size'],
                'max_duration': chunk["chunk_duration_str"],
            }
            request_list.append(dict)
    return request_list



def divide_time_range(start_datetime: datetime, end_datetime: datetime, bar_size: str):
    logging.debug("Inside divide_time_range function, fn_16")

    # Calculate the total duration of the requested time range
    total_duration = (end_datetime - start_datetime).total_seconds()

    # Get the maximum allowed duration lookback from IB based on bar size
    restricted_duration_str = maximum_duration_time_when_requesting_bars(bar_size)

    # Convert the maximum allowed duration to seconds
    restricted_duration = get_duration_seconds_from_duration_str(restricted_duration_str)


    # Initialize the list to store duration lookback requests
    duration_lookback_request_list = []

    # Check if the total duration is greater than or equal to the maximum allowed duration
    if total_duration >= restricted_duration:
        duration_lookback_request_list.append(restricted_duration_str)
        time_reminder = total_duration - restricted_duration
    else:

        current_duration_str = get_first_duration_reminder(total_duration)
        duration_lookback_request_list.append(current_duration_str)
        current_duration = get_duration_seconds_from_duration_str(current_duration_str)
        time_reminder = total_duration-current_duration

    # Get the minimum duration for the specified bar size
    min_duration_str = minimum_duration_str_when_requesting_bars(bar_size)
    min_duration_for_barsize_sec = get_duration_seconds_from_duration_str(min_duration_str)

    # print("bar_size:", bar_size)
    # print("min_duration_str:", min_duration_str)
    # print("min_duration_for_barsize_sec:", min_duration_for_barsize_sec)

    # Continue until the reminder becomes smaller than the minimum duration for the specified bar size
    search_ended = False
    while not search_ended:
        if time_reminder > min_duration_for_barsize_sec:
            current_duration_str = get_first_duration_reminder(time_reminder)
            current_duration = get_duration_seconds_from_duration_str(current_duration_str)
            duration_lookback_request_list.append(current_duration_str)
            time_reminder -= current_duration
        else:
            duration_lookback_request_list.append(current_duration_str)
            search_ended = True

    # Initialize the list to store chunks of time
    chunks = []
    chunk_end = end_datetime

    # print("start_datetime,end_datetime:",start_datetime,end_datetime)
    # Construct chunks by going backwards in time using duration lookback requests
    for duration_str in duration_lookback_request_list:
        chunk = {}
        duration = get_duration_seconds_from_duration_str(duration_str)
        chunk_start = chunk_end - timedelta(seconds=duration)
        chunk['chunk_start']=chunk_start
        # print("chunk_start:",chunk_start)
        chunk['chunk_end']=chunk_end
        # print("chunk_end:",chunk_end)
        chunk['chunk_duration_str']=duration_str
        # print("duration_str:",duration_str)
        chunk['chunk_duration']=duration
        chunks.append(chunk)
        chunk_end = chunk_start
    # print("****************************************")
    return chunks


def get_first_duration_reminder(duration_reminder):
    logging.debug("Inside get_first_duration_reminder, fn_17")
    #print("duration_reminder:", duration_reminder)
    duration_list = list(allowable_duration.items())
    #print("duration_list:", duration_list)
    list_index = 0
    for index, (duration_str, duration) in enumerate(allowable_duration.items()):
        if duration[0][0] <= duration_reminder <= duration[0][1]:
            list_index = index
            break
    #print("duration_list[list_index -1][0]", duration_list[list_index -1][0])
    return duration_list[list_index-1][0]

def minimum_duration_str_when_requesting_bars(bar_size: str):
    logging.debug(" inside minimum_duration_time_when_requesting_bars, fn_18")
    min_duration = None  # Initialize min_duration to None
    
    if bar_size in allowable_bar_sizes:
        bar_size_seconds = allowable_bar_sizes[bar_size]
        if bar_size_seconds == 30:
            min_duration = "1800 S"
        elif bar_size_seconds == 60:
            min_duration = "1800 S"
        elif bar_size_seconds == 120:
            min_duration = "1800 S"
        elif bar_size_seconds == 180:
            min_duration = "1800 S"
        elif bar_size_seconds == 300:
            min_duration = "1800 S"
        elif bar_size_seconds == 600:
            min_duration = "14400 S" 
        elif bar_size_seconds == 900:
            min_duration = "14400 S"
        elif bar_size_seconds == 1200:
            min_duration = "14400 S"    
        elif bar_size_seconds == 1800:
            min_duration = "28800 S"
        elif bar_size_seconds == 3600:
            min_duration = "1 D"
        elif bar_size_seconds == 7200:
            min_duration = "1 D"
        elif bar_size_seconds == 3*60*60:
            min_duration = "1 D"
        elif bar_size_seconds == 4*60*60:
            min_duration = "1 W"
        elif bar_size_seconds == 8*60*60:
            min_duration = "1 W"
        elif bar_size_seconds == 24*60*60:
            min_duration = "1 W"
        elif bar_size_seconds == 24*7*60*60:
            min_duration = "1 M"
        elif bar_size_seconds == 24*30*60*60:
            min_duration = "1 Y"
        else:
            raise ValueError("Invalid bar_size_seconds: " + str(bar_size_seconds))
    else:
        raise ValueError("Invalid bar_size: " + bar_size)

    return min_duration





def make_data_requests(request_list, client=None):
    if DEBUG:
        print("Inside make_data_requests")  
    logging.debug("Inside make_data_requests, fn_19")
    num_requests = len(request_list)
    request_index = 0
    pacing_window_seconds = 600  # 10 minutes
    wait_time_between_requests = 3  # 3 seconds

    if DEBUG:
        pacing_window_seconds = DEBUG_pacing_window_seconds  
        wait_time_between_requests = DEBUG_wait_time_between_requests    

    done = False
    sub_counter = 0

    while request_index < num_requests and not done:
        # Start timer for max 60 requests within pacing_window_seconds
        pacing_start_time = time.time()

        while request_index < num_requests and (time.time() - pacing_start_time) < pacing_window_seconds:

            while sub_counter < 60 and (time.time() - pacing_start_time) < pacing_window_seconds:
                if request_index < num_requests:  # Check if request_index is within range
                    request = request_list[request_index]

                    # if DEBUG:
                    #     print("Request made for:", request['instrument'])
                    #     print("request_index:", request_index)
                    #     print("sub_counter:", sub_counter)
                    #     pass

                    if client:
                        client.request_data(
                            request['contract'], request['instrument'],
                            request['end_datetime'], request['max_duration'], request['bar_size'])
                    else:
                        formatted_end_datetime = request['end_datetime'].strftime('%Y%m%d %H:%M:%S') + ' US/Eastern'
                        request_data(request['contract'], request['instrument'],
                                     formatted_end_datetime, request['max_duration'], request['bar_size'])

                    request_index += 1
                    # elapsed_time = time.time() - pacing_start_time
                    # remaining_time = max(pacing_window_seconds - elapsed_time, 0)
                    sub_counter += 1
                    time.sleep(wait_time_between_requests)
                    if DEBUG:
                        if sub_counter == 59:
                            # pdb.set_trace()
                            pass

                else:
                    done = True  # Mark as done when we have processed all requests

        # Sleep for the remaining time in the pacing window
        elapsed_time = time.time() - pacing_start_time
        remaining_time = max(pacing_window_seconds - elapsed_time, 0)
        time.sleep(remaining_time)

        # Reset sub_counter for the next pacing window
        sub_counter = 0

    if DEBUG:
        print("All requests completed.")



def get_last_friday_date():
    logging.debug("Inside get_last_friday_date,fn_20")
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    temp = current_time
    while temp.weekday() != 4:
        temp = temp - timedelta(days=1)
    temp = temp.replace(hour=23, minute=59, second=59)
    return temp


def preprocess_date(date_str):
        date_str = date_str.replace(" US/Eastern", "")
        return pd.to_datetime(date_str, format='mixed')
def save_data_to_csv(contract_map, reqId, bar_size):
    logging.debug("Inside save_data_to_csv")
    #print("Inside save_data_to_csv, bar_size,reqId,contract_map =", bar_size,reqId,contract_map)
    # if DEBUG:
    #     print("Inside save_data_to_csv, bar_size =", bar_size)

    forex_instrument = False
    us_stock_instrument = False
    us_bond_instrument = False

    contract = contract_map[reqId]['contract']
    forex_pair = f"{contract.symbol}{contract.currency}"
    other_instrument = f"{contract.symbol}"

    instrument = None
    top_folder = "./historical_data"
    
    if DEBUG:
        top_folder = "./temp_historical_data"
    
    if forex_pair in allowable_forex_pairs:
        data_folder = os.path.join(top_folder, "forex")  # Use os.path.join for path handling
        forex_instrument = True
        instrument = forex_pair
        
    elif other_instrument in allowable_US_Stocks:
        data_folder = os.path.join(top_folder, "US_Stocks")  # Use os.path.join for path handling
        us_stock_instrument = True
        instrument = other_instrument
    elif other_instrument in allowable_US_Bonds:
        data_folder = os.path.join(top_folder, "US_Bonds")  # Use os.path.join for path handling
        us_bond_instrument = True
        instrument = other_instrument
    else:
        raise ValueError("Inside save_data_to_csv")

    # Create the data folder if it doesn't exist
    os.makedirs(data_folder, exist_ok=True)

    bar_size_folder_name = bar_size.replace(" ", "_")
    bar_size_filename = bar_size.replace(" ", "_")

    # Create the folder path for the specific currency pair
    instrument_folder = os.path.join(data_folder, f"{instrument}/{bar_size_folder_name}")

    # Create the currency pair folder if it doesn't exist
    os.makedirs(instrument_folder, exist_ok=True)

    # Get the bar duration from the allowable_bar_sizes dictionary
    bar_duration = allowable_bar_sizes.get(bar_size)
    
    if bar_duration is None:
        print("bar_size:", bar_size)
        raise ValueError("Inside save_data_to_csv. Valid bar_size not found")

    # Add the bar size suffix to the filename
    filename_with_suffix = f"{instrument}_{bar_size_filename}.csv"

    # Create the complete file path for the CSV file
    file_path = os.path.join(instrument_folder, filename_with_suffix)


    
    # Check if the file already exists
    if os.path.exists(file_path):
        try:
            # Load existing data from the CSV file
            existing_data = pd.read_csv(file_path)

            # Convert the 'Date' column in existing_data to datetime objects
            existing_data['Date'] = existing_data['Date'].apply(preprocess_date)

            # Create a DataFrame with new data to append
            new_data = pd.DataFrame([
                [preprocess_date(data_point.date), data_point.open, data_point.high, data_point.low,
                data_point.close, data_point.volume]
                for data_point in contract_map[reqId]['data']
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
                for data_point in contract_map[reqId]['data']:
                    writer.writerow([preprocess_date(data_point.date), data_point.open, data_point.high, data_point.low,
                                    data_point.close, data_point.volume])
        except Exception as e:
            print(f"Error occurred while creating and writing data to the CSV file: {str(e)}")


def save_data_to_txt(contract_map, reqId, bar_size):
    logging.debug("Inside save_data_to_txt")
    print("Inside save_data_to_txt, bar_size,reqId,contract_map =", bar_size,reqId,contract_map)
    # if DEBUG:
    #     print("Inside save_data_to_csv, bar_size =", bar_size)

    forex_instrument = False
    us_stock_instrument = False
    us_bond_instrument = False

    contract = contract_map[reqId]['contract']
    forex_pair = f"{contract.symbol}{contract.currency}"
    other_instrument = f"{contract.symbol}"

    instrument = None
    top_folder = "./historical_data"

    if DEBUG:
        top_folder = "./temp_historical_data"

    if forex_pair in allowable_forex_pairs:
        data_folder = os.path.join(top_folder, "forex")  # Use os.path.join for path handling
        forex_instrument = True
        instrument = forex_pair

    elif other_instrument in allowable_US_Stocks:
        data_folder = os.path.join(top_folder, "US_Stocks")  # Use os.path.join for path handling
        us_stock_instrument = True
        instrument = other_instrument
    elif other_instrument in allowable_US_Bonds:
        data_folder = os.path.join(top_folder, "US_Bonds")  # Use os.path.join for path handling
        us_bond_instrument = True
        instrument = other_instrument
    else:
        raise ValueError("Inside save_data_to_txt")

    # Create the data folder if it doesn't exist
    os.makedirs(data_folder, exist_ok=True)

    bar_size_folder_name = bar_size.replace(" ", "_")
    bar_size_filename = bar_size.replace(" ", "_")

    # Create the folder path for the specific currency pair
    instrument_folder = os.path.join(data_folder, f"{instrument}/{bar_size_folder_name}")

    # Create the currency pair folder if it doesn't exist
    os.makedirs(instrument_folder, exist_ok=True)

    # Get the bar duration from the allowable_bar_sizes dictionary
    bar_duration = allowable_bar_sizes.get(bar_size)

    if bar_duration is None:
        print("bar_size:", bar_size)
        raise ValueError("Inside save_data_to_txt. Valid bar_size not found")

    # Add the bar size suffix to the filename
    filename_with_suffix = f"{instrument}_{bar_size_filename}.txt"

    # Create the complete file path for the CSV file
    file_path = os.path.join(instrument_folder, filename_with_suffix)

    # Check if the file already exists
    if os.path.exists(file_path):
        try:
            # Load existing data from the CSV file
            existing_data = pd.read_csv(file_path)

            # Convert the 'Date' column in existing_data to datetime objects
            existing_data['Date'] = existing_data['Date'].apply(preprocess_date)

            # Create a DataFrame with new data to append
            new_data = pd.DataFrame([
                [preprocess_date(data_point.date), data_point.open, data_point.high, data_point.low,
                 data_point.close, data_point.volume]
                for data_point in contract_map[reqId]['data']
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
            print(f"Error occurred while appending data to existing TXT file: {str(e)}")
    else:
        if DEBUG:
            print("Inside save_data_to_txt, trying to create txt-file because none existed")
        try:
            # Create a new file and write the data
            with open(file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                for data_point in contract_map[reqId]['data']:
                    writer.writerow([preprocess_date(data_point.date), data_point.open, data_point.high, data_point.low,
                                     data_point.close, data_point.volume])
        except Exception as e:
            print(f"Error occurred while creating and writing data to the TXT file: {str(e)}")


def get_bar_size_str_from_bar_size_sec(bar_size):
    logging.debug("Inside bar_size_str_from_bar_size_sec")
    #print("Inside bar_size_str_from_bar_size_sec, bar_size: ", bar_size)
    for bar_size_str, bar_size_sec in allowable_bar_sizes.items():
        if bar_size_sec == bar_size:
            return bar_size_str
        else:
            raise ValueError("Inside get_bar_size_seconds_from_bar_size_str")
        



def duration_to_duration_str(duration_seconds):
    logging.debug("Inside duration_to_duration_str ")
    # Reverse parsing of the allowable_duration dictionary is 
    # employed to handle cases where the provided duration in seconds
    # does not align with a valid IB duration lookback. 
    # This approach ensures accurate mapping of duration_str to duration_seconds, 
    # preventing potential inconsistencies.
    for duration_str, duration in reversed(allowable_duration.items()):
        if duration[0][0] <= duration_seconds <= duration[0][1]:
            return duration_str
    raise ValueError("Invalid duration_seconds value inside duration_to_duration_str")



def transform_datetime_format(datetime_str):
    # Remove "US/Eastern" (timezone part) if present
    datetime_str = re.sub(r' US/Eastern', '', datetime_str)
    # Remove excessive zeros in decimals and decimal separator
    datetime_str = re.sub(r'\.\d+', '', datetime_str)
    #print("Inside transform_datetime_format,datetime_str:", datetime_str)

    return datetime_str


def parse_datetime(datetime_str, format_str):
    #print("Inside parse_datetime, datetime_str, format_str:",datetime_str,format_str)
    return datetime.strptime(datetime_str, format_str)

def format_datetime(dt):
    #print("Inside format_datetime, dt:",dt)
    #print("Inside format_datetime, dt.strftime:",dt.strftime("%Y%m%d %H:%M:%S"))
    return dt.strftime("%Y%m%d %H:%M:%S")


# IB outputs fake OHLC data for requests which include
# bars longer back than 6 months in time and we need to filter out bars
# with date 1970-01-01 00:00:00
def clean_csv_file_from_fake_bars(file_path):
    logging.debug("Inside clean_csv_file_from_fake_bars")
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Filter out rows with dates starting with 1970
    df = df[~df["Date"].str.startswith("1970-01-01 00:00:00")]

    # Write back to the CSV file
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
    except Exception as e:
        print(f"Error occurred while creating and writing data to the CSV file: {str(e)}")







request_counter = 0
def request_data(contract, instrument, end_datetime, duration, bar_size):
        logging.debug("Inside request_data")
        global request_counter
        # print("contract: ", contract)
        # print("instrument: ", instrument)
        # print("end_datetime: ", end_datetime)
        # print("duration: ", duration)
        # print("bar_size: ", bar_size)

        request_counter += 1
        datetime_now = datetime.now()
        try:
            with open("./test.csv", "a") as file:
                writer = csv.writer(file)
                if request_counter == 1:  # Only write header for the first request
                    writer.writerow(['Request_number', 'Time of request', 'instrument', 'end_datetime', 'duration', 'bar_size'])
                writer.writerow([request_counter, datetime_now, instrument, end_datetime, duration, bar_size])
        except Exception as e:
            # Handle the exception if needed
            print(f"Error occurred: {str(e)}")

        return None

def update_data(client, top_directory):
    collection_need_of_update = generate_update_list(top_directory)
    request_list = generate_request_list(collection_need_of_update)
    make_data_requests(request_list, client)
    list_of_csv_files = listAllCsvfilesPaths(top_directory)

    for file_path in list_of_csv_files:
        clean_csv_file_from_fake_bars(file_path)

def request_live_data_for_pair(client, pair, bar_size):
    # Your code to request live data for the specified pair here
    contract = Contract()
    contract.symbol = pair[:3]
    contract.currency = pair[3:]
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"  # Specify the security type
    duration = "60 S"
    now = datetime_now_NewYork_time()
    client.request_data(contract, pair, now, duration, bar_size, keepUpToDate=True)

def request_live_data_for_pair_snd(client, pair, bar_size):
    # Your code to request live data for the specified pair here
    contract = Contract()
    contract.symbol = pair[:3]
    contract.currency = pair[3:]
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"  # Specify the security type
    client.request_live_stream_data(contract, pair, bar_size)

def create_forex_data_request_gui(client, request_live_data_for_pair):
    def on_button_click():
        pair = pair_entry.get()
        bar_size = bar_size_entry.get()
        if pair and bar_size:
            request_live_data_for_pair(client, pair, bar_size)

    root = tk.Tk()
    root.title("Forex Data Request")

    pair_label = tk.Label(root, text="Enter pair:")
    pair_label.pack()
    pair_entry = Entry(root)
    pair_entry.pack()

    bar_size_label = tk.Label(root, text="Enter bar size (default is '1 min'):")
    bar_size_label.pack()
    bar_size_entry = Entry(root)
    bar_size_entry.pack()

    request_button = tk.Button(root, text="Request Live Data", command=on_button_click)
    request_button.pack()

    root.mainloop()


import threading




from ibapi.common import BarData, UNSET_DECIMAL

from datetime import datetime, timedelta

import pytz  # Import the pytz library

def has_previous_minute_bar_created(current_time, contract_map):
    # Create a timezone object for US/Eastern (New York)
    eastern_timezone = pytz.timezone('US/Eastern')

    # Calculate the datetime for the previous minute with seconds set to 00
    previous_minute_time = current_time - timedelta(minutes=1)
    previous_minute_time = previous_minute_time.replace(second=0)

    # Make previous_minute_time timezone-aware
    #previous_minute_time = eastern_timezone.localize(previous_minute_time)

    for reqId, data_dict in contract_map.items():
        for bar_data in data_dict['data']:
            # Convert the date string to a datetime object
            bar_datetime = datetime.strptime(bar_data.date, '%Y-%m-%d %H:%M:%S')

            # Make bar_datetime timezone-aware
            bar_datetime = eastern_timezone.localize(bar_datetime)

            if bar_datetime == previous_minute_time:
                return True

    return False  # Return False if no matching bar was found



from datetime import timedelta

def can_create_previous_minute_bar(current_time, tick_data):
    previous_minute_start_time = current_time - timedelta(minutes=1)
    previous_minute_start_time = previous_minute_start_time.replace(second=0, microsecond=0)  # Set microseconds to 0

    previous_minute_end_time = previous_minute_start_time.replace(second=59, microsecond=999999)  # Set microseconds to 999999

    start_time_found = None  # Initialize to None
    end_time_found = None  # Initialize to None

    for datetime_of_arrival, tick_data_entry_list in tick_data.items():
        if datetime_of_arrival <= previous_minute_start_time:
            start_time_found = datetime_of_arrival  # Store the timestamp
        if datetime_of_arrival > previous_minute_end_time:
            end_time_found = datetime_of_arrival  # Store the timestamp

    # for second_key, tick_data_entry_list in self.tick_data.items():
    #     for tick_data_entry in tick_data_entry_list:
    #         reqId = tick_data_entry['reqId']
    #         instrument = tick_data_entry['instrument']
    #         bid_price = tick_data_entry['bid_price']
    #         ask_price = tick_data_entry['ask_price']



    return start_time_found, end_time_found



def midprices_from_tick_data(start_time, stop_time, tick_data):
    mid_price_dict = {}

    for seconds_key, entries in tick_data.items():
        mid_price_entries = []
        if start_time <= seconds_key <= stop_time:
            for entry in entries:
                bid_price = entry['bid_price']
                ask_price = entry['ask_price']
                mid_price = (bid_price + ask_price) / 2

                # Create a new entry with mid price
                mid_price_entry = {
                    'reqId': entry['reqId'],
                    'instrument': entry['instrument'],
                    'mid_price': mid_price,
                    'date': seconds_key,  # Store seconds_key as 'data'
                }

                mid_price_entries.append(mid_price_entry)

            mid_price_dict[seconds_key] = mid_price_entries

    return mid_price_dict


from ibapi.common import BarData

def derive_one_minute_bar_from_mid_price_dict(mid_price_dict):
    if not mid_price_dict:
        return None  # No data to process

    # Convert the values of mid_price_dict into a list
    mid_price_entries = []
    for entries in mid_price_dict.values():
        mid_price_entries.extend(entries)

    # Initialize the open, high, low, and close prices
    open_price = mid_price_entries[0]['mid_price']
    high_price = open_price
    low_price = open_price
    close_price = mid_price_entries[-1]['mid_price']

    # Calculate tick volume
    tick_volume = len(mid_price_entries)

    # Find the highest and lowest mid prices
    for entry in mid_price_entries:
        mid_price = entry['mid_price']
        if mid_price > high_price:
            high_price = mid_price
        elif mid_price < low_price:
            low_price = mid_price

    # Format the timestamp as a string with seconds set to 00
    timestamp_str = mid_price_entries[0]['date'].strftime('%Y-%m-%d %H:%M:00')

    # Create a new BarData object
    bar = BarData()
    bar.date = timestamp_str
    bar.open = open_price
    bar.high = high_price
    bar.low = low_price
    bar.close = close_price
    bar.volume = tick_volume
    bar.wap = UNSET_DECIMAL
    bar.barCount = 0  # Assuming this is the correct way to set barCount

    return bar




def process_minute_bars(tick_data, contract_map, reqId, bar_size_str, reqId_lock, bar_size_str_lock, contract_map_lock):
    current_daytime = datetime_now_NewYork_time()

    #  Has the one-minute bar for the previous minute been created?
    previous_minute_bar_created = has_previous_minute_bar_created(current_daytime, contract_map)
    #print("previous_minute_bar_created:", previous_minute_bar_created)
    if not previous_minute_bar_created:
        #print("previous_minute_bar_created inside if:", previous_minute_bar_created)
        # 3a. If no, can it be created?
        start_time, stop_time = can_create_previous_minute_bar(current_daytime, tick_data)
        print("start_time, stop_time:",start_time, stop_time)
        if start_time is not None and stop_time is not None:

            mid_price_dict = midprices_from_tick_data(start_time, stop_time, tick_data)
            bar = derive_one_minute_bar_from_mid_price_dict(mid_price_dict)

            #Append the bar
            with contract_map_lock, reqId_lock:
                if reqId in contract_map:
                    contract_map[reqId]['data'].append(bar)

            # Step 7: Call save_data_to_csv or save_data_to_txt
            with contract_map_lock, bar_size_str_lock:
                save_data_to_txt(contract_map, reqId, bar_size_str)

    #print("Leaving process_minute_bars")




def main():
    #update_list = generate_update_list("./historical_data")
    #request_list = generate_request_list(update_list)
   # make_data_requests(request_list)
    pass

if __name__ == "__main__":
    main()
