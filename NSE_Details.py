import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import date
import warnings
import pymongo
import logging

warnings.filterwarnings('ignore')

class StockScreener:
    def __init__(self, stockTickers, indexTicker, rfRate=0.0, tradingDays=252, db_uri='mongodb://localhost:27017/', db_name='stock_data'):
        self.stockTickers = stockTickers
        self.indexTicker = indexTicker
        self.rfRate = rfRate
        self.tradingDays = tradingDays
        self.spotData = None
        self.indexData = None
        self.additionalData = {}
        self.client = pymongo.MongoClient(db_uri)
        self.db = self.client[db_name]
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def ImportData(self):
        try:
            self.spotData = yf.download(self.stockTickers, period='max', progress=False).fillna(method='bfill')
            self.indexData = yf.download(self.indexTicker, period='max', progress=False).fillna(method='bfill')

            # Renaming columns for the index data
            self.indexData = self.indexData.rename(columns={'Open': 'Index Open', 'High': 'Index High',
                                                            'Low': 'Index Low', 'Close': 'Index Close',
                                                            'Adj Close': 'Index Adj Close', 'Volume': 'Index Volume'})

            # Joining DataFrames
            self.spotData = self.spotData.join(self.indexData[['Index Open', 'Index High', 'Index Low', 
                                                               'Index Close', 'Index Adj Close', 'Index Volume']])
            self.logger.info("Data import successful.")
        except Exception as e:
            self.logger.error(f"Error importing data: {e}")

    def FetchAdditionalData(self):
        try:
            for ticker in self.stockTickers:
                stock = yf.Ticker(ticker)
                info = stock.info
                self.additionalData[ticker] = {
                    'Market Cap': info.get('marketCap'),
                    'Dividend Yield': info.get('dividendYield'),
                    'Current Price': info.get('currentPrice'),
                    'Book Value': info.get('bookValue'),
                    'Face Value': info.get('faceValue', None),
                    'ROCE': info.get('returnOnAssets', None),
                    'ROE': info.get('returnOnEquity', None),
                    'P/E Ratio': info.get('forwardPE', info.get('trailingPE')),
                    'Forward P/E Ratio': info.get('forwardPE'),
                    'EPS': info.get('trailingEps'),
                    'Beta': info.get('beta'),
                    'Revenue': info.get('totalRevenue'),
                    'Gross Profit': info.get('grossProfits'),
                    'EBITDA': info.get('ebitda'),
                    'Debt to Equity Ratio': info.get('debtToEquity', None),
                    'Current Ratio': info.get('currentRatio', None),
                    'Quick Ratio': info.get('quickRatio', None),
                    'Free Cash Flow': info.get('freeCashflow'),
                    'Operating Cash Flow': info.get('operatingCashflow'),
                    'Price to Book Ratio': info.get('priceToBook'),
                    'PEG Ratio': info.get('pegRatio'),
                    'Return on Assets (ROA)': info.get('returnOnAssets'),
                    'Tie-up Companies': info.get('companyOfficers', 'Not Available')
                }
            self.logger.info("Additional data fetch successful.")
        except Exception as e:
            self.logger.error(f"Error fetching additional data: {e}")

    def display_additional_data(self):
        for ticker, data in self.additionalData.items():
            print(f"Data for {ticker}:")
            for key, value in data.items():
                print(f"  {key}: {value}")
            print()

    def store_data_to_mongodb(self):
        try:
            spot_data_dict = self.spotData.to_dict('index')
            self.db.spot_data.insert_many([{'date': date, **data} for date, data in spot_data_dict.items()])

            for ticker, data in self.additionalData.items():
                self.db.additional_data.update_one({'ticker': ticker}, {'$set': data}, upsert=True)
            self.logger.info("Data successfully stored to MongoDB.")
        except Exception as e:
            self.logger.error(f"Error storing data to MongoDB: {e}")

    def plot_data(self, ticker):
        try:
            if ticker in self.spotData.columns.levels[1]:
                self.spotData[ticker]['Adj Close'].plot(title=f'Adjusted Close Price of {ticker}', figsize=(12, 6))
                plt.xlabel('Date')
                plt.ylabel('Adjusted Close Price')
                plt.show()
            else:
                self.logger.warning(f"No data found for ticker: {ticker}")
        except Exception as e:
            self.logger.error(f"Error plotting data for {ticker}: {e}")

# Prompt the user for stock tickers
stockTickers = input("Enter stock ticker: ").split(',')
stockTickers = [ticker.strip() for ticker in stockTickers]  # Remove any extra whitespace
indexTicker = '^NSEI'  # Example index ticker for NIFTY 50
screener = StockScreener(stockTickers, indexTicker)
screener.ImportData()
screener.FetchAdditionalData()
screener.display_additional_data()
screener.store_data_to_mongodb()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Print the entire dataset
print(screener.spotData)

# Plot data for the first ticker
screener.plot_data(stockTickers[0])
