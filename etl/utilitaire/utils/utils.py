# coding: utf-8

import json
import logging
import os


# retourne la config d'un serveur au format dictionnaire
def read_config_sftp(path_in, server_name) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["sftp"]
    server_config = {}
    for server in L_ret :
        if server["server"] == server_name :
            server_config = server.copy()
    logging.info("Lecture config SFTP " + path_in + ".")
    return server_config


# Lecture du paramétrage de la bdd
def read_config_db(path_in, server="LOCAL SERVER"):
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["sqlite_db"]
    param_config = {}
    for param in L_ret :
        if param["server"] == server :
            param_config = param.copy()
    print("Lecture configuration serveur " + path_in + ".")
    return param_config


# Lecture de la configuration du serveur SFTP avec le compte en écriture
def read_config_ecriture(path_in, server_name):
    print(" ")
    print(" --- Lancement de la lecture de la configuration du serveur en écriture")
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["sftp"]
    param_config = {}
    for server in L_ret:
        if server["server"] == server_name:
            server_config = server.copy()
    print(" --- Lecture de la configuration en écriture", path_in, ".")
    return server_config


# retourne le path du dossier souhaité
def read_path(path_in, folder_name):
    with open(path_in) as f:
        dict_ret = json.load(f)
    if folder_name[:4] == "sftp":
        L_ret = dict_ret["path_sftp"]
    elif folder_name[:2] == "os":
        L_ret = dict_ret["path_os"]
    
    path_config = {}
    
    for folder in L_ret:
        if folder["folder"] == folder_name:
            path_config = folder.copy()

    logging.info("Lecture path" + path_in + ".")
    
    return path_config


def read_filepath(path_in, file_name):
    """
    Fonction permettant de récupérer le chemin des 
    fichiers cibles activites.csv et personnes.csv.
    """
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["file_to_transform_export"]
    file_config = {} 
    for file in L_ret:
        if file["file"] == file_name:
            file_config = file.copy()
    logging.info("Lecture path " + path_in + ".")
    return file_config


# retourne la config pour décrypter les fichiers .gpg
def read_config_decrypt_gpg(path_in) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["gpg"]
    config = {}
    for elem in L_ret :
        config = elem.copy()
    logging.info("Lecture config GPG " + path_in + ".")
    return config


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
