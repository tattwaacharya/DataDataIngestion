import pandas
from snowflake.connector.pandas_tools import write_pandas

# Create the connection to the Snowflake database.
cnx = snowflake.connector.connect(...)

# Create a DataFrame containing data about customers
df = pandas.DataFrame([('Mark', 10), ('Luke', 20)], columns=['name', 'balance'])

# Write the data from the DataFrame to the table named "customers".
success, nchunks, nrows, _ = write_pandas(cnx, df, 'customers')