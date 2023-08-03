from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import datetime
import pytz

class MyWrapper(EWrapper):
    def __init__(self):
        self.contract_map = {}
        self.data_received = False
        self.line_number = 1  # Initialize line number to 1

    def historicalData(self, reqId, bar):
        contract = self.contract_map[reqId]
        line_number = self.line_number  # Get the current line number
        print(f"Line {line_number}: Request ID: {reqId}, Instrument: {contract.symbol} {contract.currency} @ {contract.exchange}, Date/Time: {bar.date}, Open: {bar.open}, High: {bar.high}, Low: {bar.low}, Close: {bar.close}, Volume: {bar.volume}")
        self.line_number += 1  # Increment the line number for the next bar

    def historicalDataEnd(self, reqId, start, end):
        super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd. ReqId:", reqId)
        self.data_received = True

class MyClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

    def request_data(self, contract, duration_str):
        # Get the current date in US/Eastern time zone
        eastern_tz = pytz.timezone('US/Eastern')
        eastern_now = datetime.datetime.now(eastern_tz)
        end_date_time = eastern_now.strftime("%Y%m%d %H:%M:%S")

        # Calculate the start date based on the duration
        duration = int(duration_str.split()[0])  # Extract the number from the duration_str
        start_date = eastern_now - datetime.timedelta(days=duration)
        #query_time = start_date.strftime("%Y%m%d %H:%M:%S")
        query_time = start_date.strftime("%Y%m%d %H:%M:%S") + " US/Eastern"  # Add the explicit time zone
        #query_time = "20230704 00:00:00 US/Eastern"  # Override with the specific date for testing
        print("query_time: ", query_time)

        reqId = len(self.wrapper.contract_map) + 1
        self.wrapper.contract_map[reqId] = contract
        print(f"Requesting data for {contract.symbol} {contract.currency} from {query_time} to {end_date_time} with a bar size of 1 day.")
        self.reqHistoricalData(reqId, contract, query_time, duration_str, "1 day", "MIDPOINT", 1, 1, False, [])

def main():
    wrapper = MyWrapper()
    client = MyClient(wrapper)
    client.connect("127.0.0.1", 7497, clientId=0)
    # Check if connected
    print(f"Connecting to IB Gateway/TWS. Is connected: {client.isConnected()}")

    contract = Contract()
    contract.symbol = "EUR"
    contract.currency = "GBP"
    contract.exchange = "IDEALPRO"
    contract.secType = "CASH"  # Specify the security type
    duration_str = "30 D"  # Requesting data for the last 30 days
    client.request_data(contract, duration_str)

    # Wait for historical data to be received
    while not wrapper.data_received:
        client.run()  # Process IB messages
        time.sleep(0.1)

    client.disconnect()

if __name__ == "__main__":
    main()
