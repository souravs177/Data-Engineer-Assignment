
# Import necessary libraries
import pandas as pd
from datetime import datetime, date

# Function to load and standardize data
def standardize_data(df, country, id_col, name_col, dob_col, vac_type_col, vac_date_col):
    # Rename columns to standard names
    standardized_df = df.rename(columns={
        id_col: "ID",
        name_col: "Name",
        dob_col: "DOB" if dob_col else None,
        vac_type_col: "VaccinationType",
        vac_date_col: "VaccinationDate"
    })

    # Add Country column
    standardized_df["Country"] = country

    # Ensure DOB and VaccinationDate are in datetime format, if applicable
    if "DOB" in standardized_df.columns:
        standardized_df["DOB"] = pd.to_datetime(standardized_df["DOB"], errors="coerce", format="%Y-%m-%d")
    standardized_df["VaccinationDate"] = pd.to_datetime(standardized_df["VaccinationDate"], errors="coerce", format="%Y-%m-%d")

    return standardized_df

# Function to calculate age from DOB
def calculate_age(dob):
    if pd.isna(dob):
        return None
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

# Load datasets
usa_file = "/content/USA (1) 1(in).xlsx"
aus_file = "/content/AUS (1) 1(Sheet1).xlsx"
ind_file = "/content/IND (1) 1(in).xlsx"

usa_data = pd.read_excel(usa_file, sheet_name=None)
aus_data = pd.read_excel(aus_file, sheet_name=None)
ind_data = pd.read_excel(ind_file, sheet_name=None)

# Extract the first sheet from each file
usa_df = usa_data[list(usa_data.keys())[0]]
aus_df = aus_data[list(aus_data.keys())[0]]
ind_df = ind_data[list(ind_data.keys())[0]]

# Standardize datasets
usa_standardized = standardize_data(usa_df, "USA", "ID", "Name", None, "VaccinationType", "VaccinationDate")
aus_standardized = standardize_data(aus_df, "AUS", "Unique ID", "Patient Name", "Date of Birth", "Vaccine Type", "Date of Vaccination")
ind_standardized = standardize_data(ind_df, "IND", "ID", "Name", "DOB", "VaccinationType", "VaccinationDate")

# Combine datasets
combined_data = pd.concat([usa_standardized, aus_standardized, ind_standardized], ignore_index=True)

# Add derived columns: Age and Days Since Last Consulted
combined_data["Age"] = combined_data["DOB"].apply(calculate_age)
combined_data["Days_Since_Last_Consulted"] = combined_data["VaccinationDate"].apply(
    lambda x: (date.today() - x.date()).days if not pd.isna(x) else None
)

# Validate and clean data
# Remove duplicates, keeping the latest record based on "VaccinationDate"
combined_data = combined_data.sort_values(by=["ID", "VaccinationDate"], ascending=[True, False])
combined_data = combined_data.drop_duplicates(subset="ID", keep="first")

# Save the cleaned and processed data to a new file
combined_data.to_csv("processed_data.csv", index=False)

# Generate SQL table creation query
def generate_sql_table_query(table_name, columns):
    query = f"CREATE TABLE {table_name} (\n"
    for col_name, col_type in columns.items():
        query += f"    {col_name} {col_type},\n"
    query = query.rstrip(",\n") + "\n);"
    return query

# Define table schema
columns = {
    "ID": "VARCHAR(18)",
    "Name": "VARCHAR(255)",
    "DOB": "DATE",
    "VaccinationType": "CHAR(5)",
    "VaccinationDate": "DATE",
    "Country": "VARCHAR(5)",
    "Age": "INT",
    "Days_Since_Last_Consulted": "INT"
}

# Generate SQL query for each country
table_queries = {
    country: generate_sql_table_query(f"Table_{country}", columns)
    for country in combined_data["Country"].unique()
}

# Save the queries to a file
with open("table_queries.sql", "w") as f:
    for country, query in table_queries.items():
        f.write(f"-- {country} Table\n")
        f.write(query + "\n\n")

# Print success message
print("Data processing completed. Files saved:")
print("- processed_data.csv")
print("- table_queries.sql")

