# coding: utf-8

# Modules
import json
import logging
import os


def read_settings(path_in, dict, elem):
    """
    Permet de lire le document settings et retourne les informations souhaitées au format dictionnaire.

    Paramètres :
        - path_in : Chemin du dossier settings où sont stockées les informations.
        - dict : Dictionnaire contenant les informations que l'on recherche.
        - elem : Elément au sein du dictionnaire dont on souhaite retourner les informations.
    """
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret[dict]
    param_config = {}
    for param in L_ret:
        if param["name"] == elem:
            param_config = param.copy()
    logging.info("Lecture param config" + path_in + ".")
    return param_config


# def get_filenames_from_sftp(sftp):
def get_filenames_from_sftp(sftp, path_sftp) :
    """
    Fonction permettant de récupérer le nom des 
    fichiers .gpg et .csv présents au sein du sftp
    
    get_filenames_from_sftp(sftp, path_sftp)[0] : Récupère uniquement fichiers .gpg
    get_filenames_from_sftp(sftp, path_sftp)[1] : Récupère uniquement fichiers .csv
    """
    # Récupération des noms des fichiers sources
    directory_structure = sftp.listdir_attr(path_sftp)
    dict_filenames = [attr.filename for attr in directory_structure]
    gpg_files=[]
    csv_files=[]

    # Boucle permettant de ne récupérer que les fichiers .csv
    for elem in dict_filenames:
        if elem[-4::]=='.gpg':
            gpg_files.append(elem)
        elif elem[-4::]=='.csv':
            csv_files.append(elem)

    return gpg_files, csv_files


# Fonction pour récupérer nom des fichiers .gpg
def get_filenames_from_os(path):
    """
    Fonction permettant de récupérer le nom des 
    fichiers .gpg et .csv présents au sein d'un dossier de l'OS
    
    get_filenames_from_os(path)[0] : Récupère uniquement fichiers .gpg
    get_filenames_from_os(path)[1] : Récupère uniquement fichiers .csv
    """
    dict_filenames = os.listdir(path)
    gpg_files = []
    csv_files = []

    for elem in dict_filenames:
        if elem[-4::]=='.gpg':
            gpg_files.append(elem)
        elif elem[-4::]=='.csv':
            csv_files.append(elem)

    return gpg_files, csv_files
