import os
import csv 

directory = './'  # set the directory path
extension = '.csv'  # set the file extension

# loop through all files in the directory
for file in os.listdir(directory):
    # check if the file has the correct extension
    if file.endswith(extension):
        # extract the table name from the file name
        table_name = os.path.splitext(file)[0]

        with open(directory+file, newline='',encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            header = next(reader)
            quoted_header = ','.join([f'`{h}`' for h in header])
        
        # create the LOAD DATA INFILE statement
        load_data_statement = f"LOAD DATA INFILE '{directory}{file}' INTO TABLE {table_name} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS ({quoted_header});"
        
        # write the statement to a file
        with open('load_data.sql', 'a') as f:
            f.write(load_data_statement + '\n')
