import csv

# open the input file and create a csv reader object
with open('../stale/district_testing.csv', 'r', encoding='utf-8') as input_file:
    reader = csv.reader(input_file)
    header = next(reader)[6:]
    # skip the header row

    # initialize a list to hold the output rows
    output_rows = []
    
    # iterate over the rows in the input file
    for row in reader:
        
        # extract the relevant data from the row
        district_key = row[5]
        date_columns = row[6:]
        district = district_key

        # iterate over the date columns and add them to the output list
        for i in range(len(date_columns)):
            date_column = header[i]
            key = f'{district_key}_{date_column}'
            value = row[i+6]
            date_status = date_column.split("_")
            output_rows.append([district, date_status[0], date_status[1], value])
            
# write the output to a csv file
with open('../stale/district_testing_count1.csv', 'w', newline='', encoding='utf-8') as output_file:
    writer = csv.writer(output_file)
    writer.writerow(['District', 'Date', 'Status', 'Value'])
    writer.writerows(output_rows)
