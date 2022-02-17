# coding: utf-8

# Modules
import gnupg
import os

from utils import *


# Suppression des fichiers csv déjà existant dans ndata/input avant le décryptage
def delete_old_csv_files_in_os(path):
    """
    Fonction permettant de supprimer les anciens fichiers .csv présents
    dans le répertoire data/input de l'OS avant décryptage des nouveaux
    fichiers .gpg.
    """  
    gpg_files_in_os, csv_files_in_os = utils.get_filenames_from_os(path)
    
    gpg_files = []
    for elem in gpg_files_in_os:
        gpg_files.append(elem[:-4])

    files_to_delete = set(gpg_files) & set(csv_files_in_os)

    if len(files_to_delete) != 0:
        print(" --- gpg_files_in_os :", gpg_files_in_os)
        print(" ")
        print(" --- csv_files_in_os :", csv_files_in_os)
        print(" ")
        print(" --- files_to_delete :", files_to_delete)
        print(" ")

        for file in files_to_delete:
            os.remove(path + file)
            print(" --- Ancien fichier :", file, "--> supprimé")
    else:
        print(" --- Aucun fichier existant au sein du dossier data/input de l'os")


def decrypt_file(path_to_decrypt, path_gpg, password_gpg):
    """
    Fonction permettant de décripter les fichiers .gpg importés du SFTP
    en fichiers .csv au sein du répertoire data/input de l'OS.

    Param : 
        - path_to_decrypt : Dossier OS au sein duquel se trouve les fichiers .gpg à décrypter.
        - path_gpg : Chemin d'installation du programme.
        - password_gpg : Mot de passe permettant de décrypter les fichiers.
    """
    print(" ")
    print(" ------------------------------------ ")
    print(" --- Decryptage des fichiers .gpg --- ")
    print(" ------------------------------------ ")

    gpg = gnupg.GPG(path_gpg)

    # Récupération du nom des fichiers .gpg qui viennent d'être 
    # téléchargés au sein de data/input
    path = path_to_decrypt
    files_to_decrypt = utils.get_filenames_from_os(path)[0]
    print(" ")

    # Suppression des anciens fichiers .csv avant décryptage des nouveaux fichiers .gpg
    print(" --- Suppression des anciens fichiers .csv présent dans data/input avant decryptage --- ")
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
            status = gpg.decrypt_file(f, passphrase = password_gpg, output = path + newfile)

        print(status.ok)
        print(status.stderr)

        print(" --- Fichier", file, "decrypté ---")
        print(" -------------------------------------------------------------------------- ")
        print(" ")

    print(" --- Décryptage des fichiers au sein de data/input terminé --- ")
