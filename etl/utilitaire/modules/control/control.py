import pandas as pd

from modules import route_sftp

def test_not_null(path):

    list_file = []
    list_file = route_sftp.get_filenames_from_os(path)[1]

    print(" --- list_file to control :", list_file)

    for file in list_file:
        print(" ")
        print(" --- file :", file)

        filepath = path + file
        
        df = pd.read_csv(filepath, dtype=str, sep = ";")
        df.info(show_counts = True)
