from mftool import Mftool

# Initialize the Mftool object
mf = Mftool()

# Fetch all mutual fund schemes
def get_all_schemes():
    schemes = mf.get_scheme_codes()
    print("List of mutual fund schemes:")
    for code, name in schemes.items():
        print(f"{code}: {name}")
    return schemes

# Fetch details of a specific scheme
def get_scheme_details(scheme_code):
    details = mf.get_scheme_details(scheme_code)
    print("\nScheme Details:")
    for key, value in details.items():
        print(f"{key}: {value}")

# Fetch NAV data of a specific scheme
def get_nav(scheme_code):
    nav_data = mf.get_scheme_quote(scheme_code)
    print("\nNAV Details:")
    for key, value in nav_data.items():
        print(f"{key}: {value}")

# Example usage
if __name__ == "__main__":
    # Get all schemes (optional, uncomment to see full list)
    # all_schemes = get_all_schemes()

    # Ask user to enter scheme code manually
    scheme_code = input("Please enter a valid scheme code: ")

    # Fetch scheme details and NAV
    get_scheme_details(scheme_code)
    get_nav(scheme_code)
