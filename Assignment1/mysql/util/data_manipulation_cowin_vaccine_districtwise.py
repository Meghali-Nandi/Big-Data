import csv

# open the input file and create a csv reader object
with open('cowin_vaccine_data_districtwise.csv', 'r') as input_file:
    reader = csv.reader(input_file)
    header = next(reader)[6:]
    # skip the header row
    
    
    # initialize a dictionary to hold the output
    output = {}
    
    # iterate over the rows in the input file
    for row in reader:
        
        # extract the relevant data from the row
        district_key = row[3]
        date_columns = row[6:]
        district = district_key

        # iterate over the date columns and add them to the output dictionary
        for i in range(len(date_columns)):
            date_column = header[i]
            key = f'{district_key}_{date_column}'
            value = row[i+6]
            output[key] = value
            
# write the output to a csv file
with open('cowin_vaccine_districtwise_count.csv', 'w', newline='') as output_file:
    writer = csv.writer(output_file)
    writer.writerow(['CK', 'Value'])
    for key, value in output.items():
        writer.writerow([key, value])
