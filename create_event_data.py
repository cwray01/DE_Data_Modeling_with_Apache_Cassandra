"""
Takes all individual event_data files and concatenates them.
"""

import pandas as pd

import utils


def main():
    """
    Reads all event_data files,
    concatenates them to one dataframe
    and writes them into file.
    """
    print("Reading event_data files...")
    event_data_new = pd.concat(
        [utils.read_csv_file(file)
         for file in utils.get_csv_from_dir("event_data")])
    print("Writing event_data_new file...")
    event_data_new.to_csv("event_data_new.csv", index=False)
    print("Done.")


if __name__ == "__main__":
    main()
