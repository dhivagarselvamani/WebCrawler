from mftool import Mftool
import logging
from datetime import datetime
from pymongo import MongoClient  # Importing pymongo for MongoDB operations

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def calculate_yearly_profit_percentage(historical_nav):
    """
    Calculate year-wise profit percentages from historical NAV data.

    Args:
        historical_nav (dict): Historical NAV data with date and NAV values.

    Returns:
        dict: Year-wise profit percentages (positive or negative).
    """
    yearly_nav = {}

    for date_str, nav in historical_nav.items():
        try:
            date_obj = datetime.strptime(date_str, "%d-%m-%Y")
            year = date_obj.year

            if year not in yearly_nav:
                yearly_nav[year] = []
            yearly_nav[year].append(float(nav))
        except ValueError:
            logging.error(f"Invalid date format: {date_str}")

    yearly_profit = {}
    for year, navs in yearly_nav.items():
        if len(navs) < 2:
            logging.warning(f"Insufficient NAV data for year {year}.")
            continue

        start_nav = navs[0]
        end_nav = navs[-1]
        profit_percentage = ((end_nav - start_nav) / start_nav) * 100
        yearly_profit[year] = profit_percentage

    return yearly_profit


def get_scheme_details(scheme_code: str, balance_units: float = 445.804, 
                       monthly_sip: int = 2000, investment_in_months: int = 51):
    """
    Fetch and display details for a given mutual fund scheme code.
    
    Args:
        scheme_code (str): The scheme code for the mutual fund.
        balance_units (float): Number of balance units (default 445.804).
        monthly_sip (int): Monthly SIP amount in INR (default 2000).
        investment_in_months (int): Total months of investment (default 51).
        
    Returns:
        dict: A dictionary containing scheme details or errors.
    """
    mf = Mftool()
    details = {}

    try:
        logging.info("Fetching basic scheme details...")
        basic_details = mf.get_scheme_details(int(scheme_code))
        details['basic_scheme_details'] = basic_details

        logging.info(f"Fetching historical NAV for scheme code {scheme_code}...")
        historical_nav_df = mf.get_scheme_historical_nav(scheme_code, as_Dataframe=True)

        # Ensure the column name is identified correctly
        nav_column = historical_nav_df.columns[-1]  # Assuming the last column contains NAV
        historical_nav = historical_nav_df[nav_column].dropna().to_dict()
        details['historical_nav'] = historical_nav

        logging.info("Calculating year-wise profit percentages...")
        details['yearly_profit_percentages'] = calculate_yearly_profit_percentage(historical_nav)

        logging.info("Calculating balance units value...")
        details['balance_units_value'] = mf.calculate_balance_units_value(int(scheme_code), balance_units)

        logging.info("Fetching AMC details...")
        details['amc_profiles'] = mf.get_all_amc_profiles(as_json=True)

    except Exception as e:
        logging.error(f"An error occurred while fetching scheme details: {e}")
        details['error'] = str(e)

    return details


def convert_keys_to_strings(data):
    """
    Recursively convert all keys in a dictionary to strings.

    Args:
        data (dict): The input dictionary with keys that might not be strings.

    Returns:
        dict: A new dictionary with all keys converted to strings.
    """
    if isinstance(data, dict):
        return {str(key): convert_keys_to_strings(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_keys_to_strings(item) for item in data]
    else:
        return data


def store_in_mongodb(data, db_name="mutual_fund", collection_name="scheme_details"):
    """
    Store data into MongoDB.

    Args:
        data (dict): The data to store in MongoDB.
        db_name (str): The database name.
        collection_name (str): The collection name.
    """
    try:
        # Convert all keys to strings
        sanitized_data = convert_keys_to_strings(data)
        
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client[db_name]
        collection = db[collection_name]

        # Insert data into the collection
        result = collection.insert_one(sanitized_data)
        logging.info(f"Data successfully inserted with ID: {result.inserted_id}")
        return True
    except Exception as e:
        logging.error(f"An error occurred while storing data in MongoDB: {e}")
        return False


if __name__ == "__main__":
    # Collect only the scheme code from the user
    print("Mutual Fund Analysis Tool")
    print("=========================")
    scheme_code = input("Enter the Scheme Code: ").strip()

    # Fetch scheme details
    scheme_details = get_scheme_details(scheme_code=scheme_code)

    # Store in MongoDB
    if store_in_mongodb(scheme_details):
        print("\nScheme data has been successfully stored in MongoDB.")
    else:
        print("\nFailed to store scheme data in MongoDB.")

    # Display scheme details to the user
    print("\nScheme Details:")
    for key, value in scheme_details.items():
        print(f"{key}: {value}")
