

import importlib
import utility_functions

# Reload the utility_functions module
importlib.reload(utility_functions)
from ibapi.contract import Contract

import pandas as pd
import os
import re


def generate_listAllCsvfilesPaths(directory, path=""):
    csv_file_collection = pd.DataFrame()  # Create a local DataFrame

    dirs = os.listdir(directory)
    csv_files_found = False

    for item in dirs:
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path) and item.endswith('.csv'):
            csv_files_found = True
            csv_to_append = pd.DataFrame({'file_path': [item_path]})
            #print(item_path)
            csv_file_collection = pd.concat([csv_file_collection, csv_to_append], ignore_index=True)

    if not csv_files_found:
        for dir in dirs:
            new_directory = os.path.join(directory, dir)
            csv_file_collection = pd.concat([csv_file_collection, generate_listAllCsvfilesPaths(new_directory)],
                                            ignore_index=True)

    return csv_file_collection  # Return the local DataFrame

def test_listAllCsvfilesPaths(directory, path=""):


    # Body of function test_listAllCsvfilesPaths
    output_expected = generate_listAllCsvfilesPaths("./historical_data")
    output_expected_sorted = sorted(output_expected['file_path'].tolist())

    output_actual = utility_functions.listAllCsvfilesPaths("./historical_data")
    output_actual_sorted = sorted(output_actual)

    try:
        assert output_expected_sorted == output_actual_sorted
        print("Testing if utility function lists all csv-files....\033[92m OK!\033[0m")

    except AssertionError:
        print("Testing if utility function lists all csv-files....\033[91m FAILED!\033[0m")




def test_file_is_empty():

    #Helper function
    def is_file_empty_alternative(file_path):
        try:
            with open(file_path, 'r') as file:
                content = file.read()
                return len(content) == 0
        except FileNotFoundError as e:
                print(f"File not found: {e}")
        except PermissionError as e:
                print(f"Permission error: {e}")
        except OSError as e:
                print(f"OS error: {e}")
        except Exception as e:
                print(f"An error occurred: {e}")

    #Body of test_file_is_empty
    df_of_file_paths = generate_listAllCsvfilesPaths("./historical_data")
    list_sorted_file_paths = sorted(df_of_file_paths['file_path'].tolist())
    expected_output_list=[]
    actual_output_list = []

    for file_path in list_sorted_file_paths:
        expected_output_list.append(is_file_empty_alternative(file_path))
        actual_output_list.append(utility_functions.file_is_empty(file_path))

    try:
        assert expected_output_list== actual_output_list
        print("Testing utility function file_is_empty....\033[92m OK!\033[0m")

    except AssertionError:
        print("Testing utility function file_is_empty....\033[91m FAILED!\033[0m")


def test_bar_size_from_file_path():
    #Helper function
    def alternative_bar_size_from_file_path(file_path):
        filename = os.path.basename(file_path)
        match = re.search(r'_(.+?)\.csv', filename)
        if match:
            bar_size = match.group(1).replace('_', ' ')
            return bar_size
        return None

    #Body of test_bar_size_from_file_path
    df_of_file_paths = generate_listAllCsvfilesPaths("./historical_data")
    list_sorted_file_paths = sorted(df_of_file_paths['file_path'].tolist())
    expected_output_list = []
    actual_output_list = []
    for file_path in list_sorted_file_paths:
        expected_output_list.append(alternative_bar_size_from_file_path(file_path))
        actual_output_list.append(utility_functions.bar_size_from_file_path(file_path))
    #print("expected_output_list:",expected_output_list)
    #print("actual_output_list:",actual_output_list)
    try:
        assert expected_output_list== actual_output_list
        print("Testing utility function bar_size_from_file_path....\033[92m OK!\033[0m")

    except AssertionError:
        print("Testing utility function bar_size_from_file_path....\033[91m FAILED!\033[0m")


def test_maximum_duration_time_when_requesting_bars():

    expected_duration_for_bar_dict = {
    "30 secs"   :   "28800 S",
    "1 min"     :   "1 D",
    "2 mins"    :   "2 D",
    "3 mins"    :   "1 W",
    "5 mins"    :   "1 W",
    "10 mins"   :   "1 W",
    "15 mins"   :   "1 W",
    "20 mins"   :   "1 W",
    "30 mins"   :   "1 M",
    "1 hour"    :   "1 M",
    "2 hours"   :   "1 M",
    "3 hours"   :   "1 M",
    "4 hours"   :   "1 M",
    "8 hours"   :   "1 M",
    "1 day"     :   "1 Y",
    "1 week"    :   "1 Y",
    "1 month"   :   "1 Y"}

    actual_duration_for_bar_dict = {}
    for bar_size, max_duration in expected_duration_for_bar_dict.items():
        actual_duration_for_bar_dict[bar_size]= \
            utility_functions.maximum_duration_time_when_requesting_bars(bar_size)

        #print("expected_duration_for_bar_dict:",expected_duration_for_bar_dict)
        #print("actual_duration_for_bar_dict:",actual_duration_for_bar_dict)
    try:
        assert expected_duration_for_bar_dict == actual_duration_for_bar_dict
        print("Testing utility function maximum_duration_time_when_requesting_bars....\033[92m OK!\033[0m")

    except AssertionError:
        print("Testing utility function maximum_duration_time_when_requesting_bars....\033[91m FAILED!\033[0m")

def test_last_OHLC_date_from_csv_file():
    file_path = "./historical_data/forex/USDCAD/1_day/USDCAD_1_day.csv"  # Replace with the actual file path
    expected_last_date = "2023-08-10 00:00:00.000000000"  # Replace with your expected value

    actual_last_date = utility_functions.last_OHLC_date_from_csv_file(file_path)
    #print("expected_last_date:",expected_last_date)
    #print("actual_last_date:",actual_last_date)

    try:
        assert expected_last_date == actual_last_date
        print("Testing utility function last_OHLC_date_from_csv_file....\033[92m OK!\033[0m")
    except AssertionError:
        print("Testing utility function last_OHLC_date_from_csv_file....\033[91m FAILED!\033[0m")

def test_clean_csv_file_from_fake_bars():
    file_path_unclean = "./historical_data/forex/USDCAD/1_day/USDCAD_1_day.txt"
    utility_functions.clean_csv_file_from_fake_bars(file_path_unclean)
    file_path_clean = "./historical_data/forex/USDCAD/1_day/USDCAD_1_day.csv"

    # Read the unclean and clean CSV files into DataFrames
    df_unclean = pd.read_csv(file_path_unclean)
    df_clean = pd.read_csv(file_path_clean)

    # Compare the two DataFrames
    if df_unclean.equals(df_clean):
        print("Testing utility function clean_csv_file_from_fake_bars....\033[92m OK!\033[0m")
    else:
        print("Testing utility function clean_csv_file_from_fake_bars....\033[91m FAILED!\033[0m")

def test_get_instrument_from_file_name():
    def alternative_get_instrument_from_file_name(file_path):
        #Example file_path: ./historical_data/forex/NZDUSD/1_day/NZDUSD_1_day.csv
        filename = os.path.basename(file_path)
        match = re.search(r'^(.*?)_', filename)
        if match:
            instrument = match.group(1)
            #print("instrument:",instrument)
            return instrument
        return None

    df_of_file_paths = generate_listAllCsvfilesPaths("./historical_data")
    list_sorted_file_paths = sorted(df_of_file_paths['file_path'].tolist())
    output_expected=[]
    output_actual =[]
    for file_path in list_sorted_file_paths:
        output_expected.append(alternative_get_instrument_from_file_name(file_path))
        output_actual.append(utility_functions.get_instrument_from_file_name(file_path))

    #print("output_expected:",output_expected)
    #print("output_actual:",output_actual)
    try:
        assert output_actual == output_expected
        print("Testing utility function get_instrument_from_file_name....\033[92m OK!\033[0m")

    except AssertionError:
        print("Testing utility function get_instrument_from_file_name....\033[91m FAILED!\033[0m")


def test_get_instrument_class_from_file_name():
    #Example filepath: "./historical_data/forex/USDCAD/1_day/USDCAD_1_day.csv"
    def alternative_get_instrument_class_from_file_name(file_path):
        # Example file_path: ./historical_data/forex/NZDUSD/1_day/NZDUSD_1_day.csv
        match = re.search(r'./historical_data/(.*?)/', file_path)
        if match:
            instrument_type = match.group(1)
            #print("instrument_type:", instrument_type)
            return instrument_type
        return None

    df_of_file_paths = generate_listAllCsvfilesPaths("./historical_data")
    list_sorted_file_paths = sorted(df_of_file_paths['file_path'].tolist())
    output_expected = []
    output_actual = []
    for file_path in list_sorted_file_paths:
        output_expected.append(alternative_get_instrument_class_from_file_name(file_path))
        output_actual.append(utility_functions.get_instrument_class_from_file_path(file_path))

    try:
        assert output_actual == output_expected
        print("Testing utility function get_instrument_type_from_file_path....\033[92m OK!\033[0m")

    except AssertionError:
        print("Testing utility function get_instrument_type_from_file_path....\033[91m FAILED!\033[0m")


def test_get_contract_from_csv_file_name():
    df_of_file_paths = generate_listAllCsvfilesPaths("./historical_data")
    list_sorted_file_paths = sorted(df_of_file_paths['file_path'].tolist())

    actual_contract_list = []
    expected_contract_list=[]

    for file_path in list_sorted_file_paths:
        instrument_class = utility_functions.get_instrument_class_from_file_path(file_path)
        instrument = utility_functions.get_instrument_from_file_name(file_path)

          # Create a new instance for each iteration

        if instrument_class == 'forex':
            # Set up expected attributes for forex contract
            expected_contract = Contract()
            expected_contract.symbol = instrument[:3]
            expected_contract.currency = instrument[3:]
            expected_contract.exchange = "IDEALPRO"
            expected_contract.secType = "CASH"
            expected_contract_list.append(expected_contract)

        elif instrument_class == 'US_Stock':
            # Set up expected attributes for US stock contract
            expected_contract = Contract()  # Create a new instance for US stock contract
            expected_contract.symbol = instrument
            expected_contract.currency = "USD"
            expected_contract.exchange = "SMART"
            expected_contract.secType = "STK"
            expected_contract.primaryExchange = "ARCA"
            expected_contract_list.append(expected_contract)

        elif instrument_class == 'US_Bond':
            # Set up expected attributes for US bond contract
            expected_contract = Contract()  # Create a new instance for US bond contract
            expected_contract.symbol = instrument
            expected_contract.currency = "USD"
            expected_contract.exchange = "SMART"
            expected_contract.secType = "BOND"
            expected_contract_list.append(expected_contract)

        # Generate the contract using the function under test
        actual_contract = utility_functions.get_contract_from_csv_file_path(file_path)
        actual_contract_list.append(actual_contract)


        #for (actual_attr, actual_value), (expected_attr, expected_value) in zip(vars(actual_contract).items(),
        #                                                                       vars(expected_contract).items()):
        #    print(f"actual_contract: {actual_attr} = {actual_value}, expected_contract: {expected_attr} = {expected_value}")


    # Loop through the contracts and compare them
    for index, (expected_contract, actual_contract) in enumerate(zip(expected_contract_list, actual_contract_list)):
        if expected_contract != actual_contract:
            print(f"Contract comparison failed at index {index}")

            print("Expected Contract:")
            for attr_name in dir(expected_contract):
                if not callable(getattr(expected_contract, attr_name)) and not attr_name.startswith("__"):
                    expected_value = getattr(expected_contract, attr_name)
                    actual_value = getattr(actual_contract, attr_name)
                    if expected_value != actual_value:
                        print(f"Attribute {attr_name}: Expected = {expected_value}, Actual = {actual_value}")

            print("Actual Contract:")
            for attr_name in dir(actual_contract):
                if not callable(getattr(actual_contract, attr_name)) and not attr_name.startswith("__"):
                    expected_value = getattr(expected_contract, attr_name)
                    actual_value = getattr(actual_contract, attr_name)
                    if expected_value != actual_value:
                        print(f"Attribute {attr_name}: Expected = {expected_value}, Actual = {actual_value}")

            print("---------------------")

    # Compare the generated contracts with the expected contracts
    try:
        assert actual_contract_list == expected_contract_list
        print("Testing utility function get_contract_from_csv_file_path....\033[92m OK!\033[0m")
    except AssertionError:
        print("Testing utility function get_contract_from_csv_file_path....\033[91m FAILED!\033[0m")


def main():
    test_listAllCsvfilesPaths("./historical_data")
    test_file_is_empty()
    test_bar_size_from_file_path()
    test_maximum_duration_time_when_requesting_bars()
    test_last_OHLC_date_from_csv_file()
    test_clean_csv_file_from_fake_bars()
    test_get_instrument_from_file_name()
    test_get_instrument_class_from_file_name()
    test_get_contract_from_csv_file_name()

if __name__ == "__main__":
    main()

# Certainly, I'd be happy to explain!

# dir(expected_contract): The dir() function returns a list of attributes and methods of the given object.
# In this case, it will return a list of attribute names for the expected_contract object.

# if not callable(getattr(expected_contract, attr_name)) and not attr_name.startswith("__"):
# This line has two conditions joined by the and logical operator:

#     not callable(getattr(expected_contract, attr_name)):
#     This condition checks if the attribute is not a callable function. It ensures that we are only comparing
#     values of non-method attributes.

#     not attr_name.startswith("__"):
#     This condition checks if the attribute name does not start with "__".
#     This is to exclude internal attributes (usually Python's special attributes) that are not relevant for comparison.

# expected_value = getattr(expected_contract, attr_name):
# This line uses the getattr() function to get the value of the attribute with the name attr_name from the expected_contract object.
# So, getattr(expected_contract, attr_name) returns the value associated with the attribute name.

# actual_value = getattr(actual_contract, attr_name):
# Similarly, this line uses getattr() to get the value of the same attribute name from the actual_contract object.

# To summarize, the code is iterating through the attributes of both the expected_contract and actual_contract objects.
# It is excluding callable attributes and internal attributes.
# For each attribute, it's getting the corresponding values from both objects and comparing them.
# If the values are not equal, it prints out the attribute name, the expected value, and the actual value for easy debugging.




