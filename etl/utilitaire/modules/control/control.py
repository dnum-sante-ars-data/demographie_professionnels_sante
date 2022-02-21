import pandas as pd

from utils import *

def test_not_null(path):
    """
    Analyse globale de fichiers .csv permettant de connaitre notamment
    le type des données et le nombre de non-null présent.

    Paramètre :
        - path : Chemin du dossier où sont stockés les fichiers .csv à analyser.
    """
    list_file = []
    list_file = utils.get_filenames_from_os(path)[1]

    print(" --- list_file to control :", list_file)

    for file in list_file:
        print(" ")
        print(" --- file :", file)

        filepath = path + file
        
        df = pd.read_csv(filepath, dtype=str, sep = ";")
        df.info(show_counts = True)
