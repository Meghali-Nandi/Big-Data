import pandas as pd
from sqlalchemy import create_engine
import os

csv_files = [filename for filename in os.listdir('./') if filename.endswith('.csv')]


for f in csv_files:
	print(f)
	# read the CSV file using pandas
	df = pd.read_csv(f,low_memory=False)

	# create a MySQL engine using sqlalchemy
	engine = create_engine('mysql+mysqlconnector://root:password@localhost:3306/assignment1')
	
	# infer the schema from the DataFrame
	schema = pd.io.sql.get_schema(df.reset_index(), f.replace('.csv',''), con=engine)
	
	# print the CREATE TABLE statement
	with open('queries.sql', 'a') as file:
		file.write(schema+";\n")

