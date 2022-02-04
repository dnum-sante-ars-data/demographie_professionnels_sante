# Modules
import gnupg
import os


# Récupération des noms des fichiers .gpg téléchargés dans data/input
def get_os_filenames(path):
    """
    Fonction permettant de récupérer la liste des noms des fichiers .gpg et .csv
    présent au sein d'un répertoire de l'OS.
    """
    dict_filenames = os.listdir(path)
    files_gpg = []
    files_csv = []

    for elem in dict_filenames:
        if elem[-4::]=='.gpg':
            files_gpg.append(elem)
        elif elem[-4::]=='.csv':
            files_csv.append(elem)
    return files_gpg, files_csv


# Suppression des fichiers csv déjà existant dans ndata/input avant le décryptage
def delete_old_csv_files_in_os(path):
    """
    Fonction permettant de supprimer les anciens fichiers .csv présents
    dans le répertoire data/input de l'OS avant décryptage des nouveaux
    fichiers .gpg.
    """
    gpg_files_in_os, csv_files_in_os = get_os_filenames(path)

    gpg_files = []
    for elem in gpg_files_in_os:
        gpg_files.append(elem[:-4])

    files_to_delete = set(gpg_files) & set(csv_files_in_os)

    if len(files_to_delete) != 0:
        print(" --- gpg_files_in_os :", gpg_files_in_os)
        print(" --- csv_files_in_os :", csv_files_in_os)
        print(" --- files_to_delete :", files_to_delete)

        for file in files_to_delete:
            os.remove(path + file)
            print(" --- Ancien fichier :", file, "--> supprimé")
    else:
        print(" --- Aucun fichier existant au sein du dossier data/input de l'os")


def decrypt_file(path):
    """
    Fonction permettant de décripter les fichiers .gpg importés du SFTP
    en fichiers .csv au sein du répertoire data/input de l'OS.

    Param : 
        path : Dossier OS au sein duquel se trouve les fichiers .gpg à décripter.
    """
    print(" ")
    print(" ------------------------------------ ")
    print(" --- Decryptage des fichiers .gpg --- ")
    print(" ------------------------------------ ")

    gpg = gnupg.GPG("/usr/bin/gpg")

    # Récupération du nom des fichiers .gpg qui viennent d'être 
    # téléchargés au sein de data/input
    path = path
    files_to_decrypt = get_os_filenames(path)[0]
    #print(" --- files_to_decrypt :", files_to_decrypt)
    print(" ")

    # Suppression des anciens fichiers .csv avant décryptage des nouveaux fichiers .gpg
    print(" --- Suppression des anciens fichiers .csv avant decryptage --- ")
    delete_old_csv_files_in_os(path)
    print(" ")

    # Boucle de décryptage des fichiers 
    print(" --- Décryptage des nouveaux fichiers .gpg --- ")

    for file in files_to_decrypt:
        print(" -------------------------------------------------------------------------- ")
        print(" --- Décryptage du fichier :", file, "--- ")
        print(" -------------------------------------------------------------------------- ")

        # newfile = nouveau nom du fichier en supprimant ".gpg" à la fin
        newfile = file[:-4]

        filepath = path + file

        with open(filepath, "rb") as f:
            status = gpg.decrypt_file(f, passphrase = "01062021@beD", output = path + newfile)

        print(status.ok)
        print(status.stderr)

        print(" --- Fichier", file, "decrypté ---")
        print(" -------------------------------------------------------------------------- ")
        print(" ")
