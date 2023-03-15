import csv

# open the input file and create a csv reader object
with open('district_testing.csv', 'r',encoding='utf-8') as input_file:
    reader = csv.reader(input_file)
    
    # open the output file and create a csv writer object
    with open('district_testing_new.csv', 'w', newline='',encoding='utf-8') as output_file:
        writer = csv.writer(output_file)
        
        # iterate over the rows in the input file
        for row in reader:
            
            # extract the first 6 columns and write them to the output file
            output_row = row[:6]
            writer.writerow(output_row)
