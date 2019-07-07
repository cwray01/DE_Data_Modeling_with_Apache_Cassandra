import pandas as pd
import os
import glob
import csv

def main():
    print(os.getcwd())
    filepath = os.getcwd() + '/event_data'

    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))
        print(file_path_list)

    full_data_row_list = []
    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)

            for line in csvreader:
                full_data_row_list.append(line)

    print(len(full_data_row_list))

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length', \
                         'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_row_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

if __name__ == "__main__":
    main()
