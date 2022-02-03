# coding: utf-8

#from fileinput import filename
import json
import logging
import pysftp
#import re
#import ntpath
#import sys
#import time
from tqdm import tqdm
#from glob import glob

import subprocess
import wget

import os
import ftplib
from progressbar import AnimatedMarker, Bar, BouncingBar, Counter, ETA, AdaptiveETA, FileTransferSpeed, FormatLabel, Percentage, ProgressBar, ReverseBar, RotatingMarker, SimpleProgress, Timer, UnknownLength

global ftp


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


# Retourne le path_sftp
def read_path_os(path_in, folder_name):
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["path_os"]
    path_config = {}
    for folder in L_ret:
        if folder["folder"] == folder_name:
            path_config = folder.copy()
    logging.info("Lecture path " + path_in + ".")
    return path_config


# Retourne le path_sftp
def read_path_sftp(path_in, folder_name):
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["path_sftp"]
    path_config = {}
    for folder in L_ret:
        if folder["folder"] == folder_name:
            path_config = folder.copy()
    logging.info("Lecture path " + path_in + ".")
    return path_config


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


def delete_old_gpg_files_in_os(sftp, path_os, path_sftp):
    """
    Fonction permettant de supprimer les anciennes versions des fichiers .gpg 
    présentent au sein du dossier data/input de l'OS, avant importation 
    des nouvelles versions depuis le SFTP. 

    Args:
        sftp : Elements de connexion au SFTP
        path_os : Direction du dossier OS où vérifier la présence ou non des fichiers à importer.
        path_sftp : Direction du dossier SFTP depuis lequel les fichiers vont être importés.
    """
    # Récupération du nom des fichiers présents dans data/input
    files_in_os = get_filenames_from_os(path = path_os)[0]
    #files_in_os = get_os_gpg_filenames(path = path_os)
    print(" --- files_in_os :", files_in_os)
    files_in_sftp = get_filenames_from_sftp(sftp, path_sftp)[0]
    #files_in_sftp = get_sftp_gpg_filenames(sftp = sftp, path_sftp = path_sftp) 
    print(" --- files_in_sftp :", files_in_sftp)

    # Comparaison entre fichiers présents dans data/input et ceux dans sftp
    files_to_delete = set(files_in_sftp) & set(files_in_os)
    print(" --- files_to_delete :", files_to_delete)

    # Suppression des fichiers présents dans data/input et qui vont être à nouveau importés
    # depuis sftp
    if len(files_to_delete) != 0:
        print(" --- files_in_sftp   :", files_in_sftp)
        print(" --- files_in_os     :", files_in_os)
        print(" --- files_to_delete :", files_to_delete)

        for file in files_to_delete:
            os.remove(path_os + file)
            print(" --- Ancien fichier :", file, "--> supprimé")
    else:
        print(" --- Aucun fichier existant au sein du dossier data/input de l'os")


# Fonction de suppression des anciens fichiers transférés vers SFTP
def delete_old_files_in_sftp(server_in_config, path_sftp, path_os):
    """
    Fonction permettant de supprimer les fichiers présents dans le dossier 
    demographie_ps/output du SFTP et dont une nouvelle version est sur le 
    point d'être transférée.
    """
    host = server_in_config["host"]
    username = server_in_config["username"]
    password = server_in_config["password"]

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host=host, username=username, password=password, port = 2222, cnopts=cnopts)

    files_in_sftp = get_filenames_from_sftp(sftp, path_sftp)[1]

    files_in_os = get_filenames_from_os(path_os)[1]

    files_to_delete = set(files_in_sftp) & set(files_in_os)

    if len(files_to_delete) != 0:
        print(" --- files_in_sftp   :", files_in_sftp)
        print(" --- files_in_os     :", files_in_os)
        print(" --- files_to_delete :", files_to_delete)
        for file in files_to_delete:
            ftp = ftplib.FTP(host, username, password)
            ftp.delete(path_sftp + "/" + file)
            print(" --- Ancien fichier :", file, "--> supprimé")
    else:
        print(" --- Aucun fichier existant au sein du dossier demographie_ps/output dans le SFTP")


# telechargement complet via wget
def save_wget_sftp(server_in_config, path_os, path_sftp):
    """Telechargement complet via wget

    Args:
        server_in_config ([dict]): Configuration du server au 
        format dictionnaire. Récupéré via read_config_sftp
    """
    print(" ")
    print(' --- Lancement de la commande wget')
    print(" ")

    host = server_in_config["host"]
    #print('host :', host)
    username = server_in_config["username"]
    #print('username :', username)
    password = server_in_config["password"]
    #print('password :', password)

    path_sftp = path_sftp["path"]
    print(path_sftp)

    path_os = path_os["path"]
    print(path_os)

    # localisation du fichier a recuperer sur le serveur sftp
    print(" --- Connexion au SFTP --- ")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        print(" ")

        # Récupération des fichiers à importer
        filenames_from_sftp = get_filenames_from_sftp(sftp, path_sftp)[0]
        #filenames_from_sftp = get_sftp_gpg_filenames(sftp, path_sftp)
        print(" -- Fichiers à importer depuis sftp : ", filenames_from_sftp)

        # Suppression des anciens fichiers importés précédemment
        print(" --- Suppression des anciens fichiers .gpg déjà présent dans data/input de l'OS --- ")
        delete_old_gpg_files_in_os(sftp, path_os = path_os, path_sftp = path_sftp)
        print(" ")

        # Boucle for pour importer nouveaux fichiers depuis sftp
        print(" --- Importation des nouveaux fichiers .gpg depuis le SFTP --- ")
        print(" ")

        for file in filenames_from_sftp:
            print(" -------------------------------------------------------------------------- ")
            print(" --- Importation du fichier :", file, "--- ")
            print(" -------------------------------------------------------------------------- ")

            dst = path_os
            sftp_path = path_sftp + file
            cmd = 'wget --directory-prefix='+dst+' --user="'+username+'" --password="'+password+'"  ftp://'+host+'/'+sftp_path+' --progress=bar'
            subprocess.run(cmd, shell=True)
            print(' --- Commande "'+cmd+'" exécutée')
            print(' --- Nouveau fichier : ', file, ' --> importé')
            print(" -------------------------------------------------------------------------- ")
            print(" ")
    return


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


# Test export_sftp
def execute_upload(server_in_config, path_os, path_sftp):
    """
    Fonction permettant de transférer un fichier présent dans le dossier "path_os" de l'os, 
    vers le dossier "path_sftp" du SFTP.
    """
    print(" ")
    print(" --- Execution de l'upload vers SFTP --- ")
    print(" ")
    host = server_in_config["host"]
    username = server_in_config["username"]
    password = server_in_config["password"]

    # Connexion au SFTP
    print(" --- Connexion au SFTP ---")
    ftp = ftplib.FTP(host, username, password)
    print(" --- Connecté au SFTP :", ftp.getwelcome())

    # Désignation du dossier cible dans SFTP
    ftpResponseMessage = ftp.cwd(path_sftp)
    print(" --- Dossier cible :", path_sftp,"->", ftpResponseMessage)
    print(" ")

    # Suppression des fichiers csv présents dans demographie_ps/output du SFTP et identiques aux fichiers qui vont être exportés vers ce même SFTP
    print(" --- Suppression des fichiers au sein du SFTP ---")
    delete_old_files_in_sftp(server_in_config = server_in_config, path_sftp = path_sftp, path_os = path_os)

    # Récupération des noms des fichiers .csv présent dans data/output à transférer vers SFTP
    files_to_sftp = get_filenames_from_os(path_os)[1]

    # Boucle permettant d'exporter un à un les fichiers présents dans data/output vers le SFTP
    for files in files_to_sftp:
        print(" ")
        print(" ----------------------------------------------- ")
        print(" --- Transfert de", files, "vers le SFTP ---")
        print(" ----------------------------------------------- ")

        filepath = path_os + files
        size_local_file = os.path.getsize(filepath)
        #print(" --- size_local_file :", size_local_file)

        file_to_transfer = open(filepath, 'rb')
        #print(" --- file_to_transfer :", file_to_transfer)

        with tqdm(unit = 'blocks', unit_scale = True, leave = True, miniters = 1, desc = 'Uploading......', total = size_local_file) as tqdm_instance:

            ftp.storbinary('STOR ' + files, file_to_transfer, 2048, callback = lambda sent: tqdm_instance.update(len(sent)))
            file_to_transfer.close()

        print(" --- Publication de", files, "vers SFTP exécutée --- ")

    ftp.quit()
    ftp = None
    print(" ")

