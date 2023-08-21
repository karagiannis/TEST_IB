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

logging.basicConfig(filename='app.log', level=logging.ERROR)


def listAllCsvfilesPaths(top_directory):
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



def file_is_empty(file_path):
    try:
        return os.path.getsize(file_path) == 0
    except OSError as e:
        logging.error(f"Error in file_is_empty while checking file size for '{file_path}': {e}")
        return False



def bar_size_from_file_path(file_path):
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
    if bar_size in allowable_bar_sizes:
        bar_size_seconds = allowable_bar_sizes[bar_size]
    largest_duration = None
    for duration, range_list in allowable_duration.items():
        if range_list[0][0] <= bar_size_seconds:
            largest_duration = duration
    return largest_duration


def last_OHLC_date_from_csv_file(file_path):
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


# IB outputs fake OHLC data for requests which include
# bars longer back than 6 months in time and we need to filter out bars
# with date 1970-01-01 00:00:00
def clean_csv_file_from_fake_bars(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Filter out rows with dates starting with 1970
    df = df[~df["Date"].str.startswith("1970-01-01 00:00:00")]

    # Write back to the CSV file
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
    except Exception as e:
        print(f"Error occurred while creating and writing data to the CSV file: {str(e)}")


# File name is for instance ./historical_data/forex/USDCAD/1_day/USDCAD_1_day.csv
# We split at '/' then take the last part and then split again and take the zeroth part
def get_instrument_from_file_name(file_path):
    # Extracting the base filename using os.path.basename
    filename = os.path.basename(file_path)
    # Splitting the filename by underscore and taking the first part
    instrument = filename.split('_')[0]
    return instrument  # Example: "EURUSD", "IBM"


# We only have the folder "forex" but suppose
# we also have ./historical_data/USStock/....
# We need to know the type of the instrument because it determines which
# exchange we are going to enter in Contract
# Contract.exchange = "IDEALPRO" for Forex or
# Contract.exchage = "SMART" for US Stock and ETFs
def get_instrument_class_from_file_path(file_path):
    instrument_class = file_path.split('/')[2]
    return instrument_class  # Example "forex" or "USStock" or "USBond"


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


def todays_week_day():
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    week_day = current_time.weekday()
    return week_day  # Example 0 = Monday, 6=Friday


def datetime_now_NewYork_time():
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    return current_time  # "20230723 15:15:00 US/Eastern"


# we will return the date for the friday 6 months ago
# with the hour of the end of the trading day
def datetime_six_months_ago_NewYork_time():
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    six_months_ago = current_time - timedelta(days=6 * 30)
    while six_months_ago.weekday() != 4:
        six_months_ago = six_months_ago + timedelta(days=1)
        six_months_ago = six_months_ago.replace(hour=23, minute=59, second=59)  # replace works on the datetime object!
    return six_months_ago  # Example "20230223 23:59:59 US/Eastern"


def get_last_friday_date():
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    temp = current_time
    while temp.weekday() != 4:
        temp = temp - timedelta(days=1)
    temp = temp.replace(hour=23, minute=59, second=59)
    return temp




# Here we assume that we have made a maximum duration request for a particular
# bar_size and make take into account the max duration to calculate a
# new end_date_time because the IBAPI does only allow you to specify a
# an end_date_time and a duration which is backwards in time.
# end_date_time = duration + start_time, where start_time is
# the end_time of the previous request
def next_datetime_for_bar_request(end_datetime, bar_size):
    eastern_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern_tz)
    duration = maximum_duration_time_when_requesting_bars(bar_size)
    new_end_time = end_datetime + timedelta(seconds=duration)
    if new_end_time > current_time:
        return current_time
    else:
        return new_end_time  # Example: "20230223 15:15:00 US/Eastern" to be used in request_date with a duration (backwards)


def generate_update_list(top_directory: str):
    collection_csv_file_paths = listAllCsvfilesPaths(top_directory)
    collection_need_of_update = []
    six_months_ago = datetime_six_moths_ago_NewYork_time()
    last_friday = get_last_friday_date()
    now = datetime_now_NewYork_time()

    for csv_file_path in collection_csv_file_paths:
        bar_size = bar_size_from_file_path(csv_file_path)
        allowable_duration_in_bar_request = maximum_duration_time_when_requesting_bars(bar_size)
        instrument = get_instrument_from_file_name(csv_file_path)
        contract = get_contract_from_csv_file_name(csv_file_path)

        if file_is_empty(csv_file_path):
            start_datetime = six_months_ago
        else:
            last_saved_bar_datetime = last_OHLC_date_from_csv_file(csv_file_path)
            start_datetime = next_datetime_for_bar_request(last_saved_bar_datetime, bar_size)

        if todays_week_day() == 5 or todays_week_day() == 6:
            end_datetime = last_friday
        else:
            end_datetime = now

        update_dict = {
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'contract': contract,
            'instrument': instrument,
            'bar_size': bar_size,
            'max_duration': allowable_duration_in_bar_request,
            'file_path': csv_file_path
        }

        collection_need_of_update.append(update_dict)

    return collection_need_of_update



def divide_time_range(start_datetime, end_datetime, bar_size):
    total_duration = (end_datetime - start_datetime).total_seconds()
    restricted_duration = maximum_duration_time_when_requesting_bars(bar_size)
    num_chunks = int(total_duration / restricted_duration)

    chunks = []
    for i in range(num_chunks):
        end_chunk_time = start_datetime + timedelta(seconds=restricted_duration)
        chunks.append((start_datetime, end_chunk_time))
        start_datetime = end_chunk_time
    return chunks


def generate_request_list(collection_list):
    request_list = []
    for start_datetime, end_datetime, contract, instrument, bar_size, max_duration, file_path in collection_list.item():
        total_duration = end_datetime - start_datetime
        chunks = divide_time_range(end_datetime, bar_size, total_duration)
        for chunk in chunks:
            dict = {
                'start_datetime': chunk[0],
                'end_datetime': chunk[1],
                'contract': contract,
                'instrument': instrument,
                'bar_size': bar_size,
                'max_duration': max_duration,
                'file_path': file_path
            }
            request_list.append(dict)
    return request_list


# Define time_periods
time_periods = []


import time

def make_data_requests(request_list):
    num_requests = len(request_list)
    request_index = 0
    pacing_request_limit = 60
    pacing_window_seconds = 600  # 10 minutes
    wait_time_between_requests = 3  # seconds

    while request_index < num_requests:
        requests_remaining = num_requests - request_index

        # Determine the number of requests to execute in this iteration
        num_to_execute = min(requests_remaining, pacing_request_limit)

        for i in range(num_to_execute):
            request = request_list[request_index]
            request_data(request['contract'], request['instrument'], request['start_datetime'],
                         request['end_datetime'], request['max_duration'], request['bar_size'])
            request_index += 1
            # Wait for the specified time between requests
            if i < num_to_execute - 1:
                time.sleep(wait_time_between_requests)

        # Wait for the remaining time within the pacing window
        if requests_remaining > pacing_request_limit:
            elapsed_time = time.time() - request_list[request_index - 1]['timestamp']
            remaining_time = pacing_window_seconds - elapsed_time
            if remaining_time > 0:
                time.sleep(remaining_time)




def request_data(contract, instrument, start_datetime, end_datetime,
                 allowable_duration_in_bar_request, bar_size):
    print("contract: ", contract)
    print("instrument: ", instrument)
    print("start_datetime: ", start_datetime)
    print("end_datetime: ", end_datetime)
    print("allowable_duration_in_bar_request: ", allowable_duration_in_bar_request)
    print("bar_size: ", bar_size)

    return None


def main():
    #update_list = generate_update_list("./historical_data")
    #request_list = generate_request_list(update_list)
   # make_data_requests(request_list)
    pass

if __name__ == "__main__":
    main()
