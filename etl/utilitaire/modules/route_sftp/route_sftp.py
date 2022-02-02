# coding: utf-8

from fileinput import filename
import json
import logging
import pysftp
import re
import ntpath
import sys
import time
from tqdm import tqdm
from glob import glob

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

# telechargement complet via wget
# def save_wget_sftp(server_in_config):
def save_wget_sftp(server_in_config, path_sftp):
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

    # localisation du fichier a recuperer sur le serveur sftp
    print(" --- Connexion au SFTP")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        print(" ")        

        # Récupération des fichiers à importer
        filenames_from_sftp = get_sftp_gpg_filenames(sftp, path_sftp)
        #print(" -- Fichiers à importer depuis sftp : ", filenames_from_sftp)
        
        # Suppression des anciens fichiers importés précédemment
        print(" --- Suppression des anciens fichiers .gpg déjà présent dans data/input de l'OS") 
        delete_old_gpg_files_in_os(sftp, path_os = "data/input", path_sftp = "demographie_ps/input")
        print(" ")

        
        # Boucle for pour importer nouveaux fichiers depuis sftp
        print(" --- Importation des nouveaux fichiers .gpg depuis le SFTP")
        print(" ")

        for file in filenames_from_sftp:
            print(" -------------------------------------------------------------------------- ")
            print(" --- Importation du fichier :", file, "--- ")
            print(" -------------------------------------------------------------------------- ")
 
            dst = "data/input"
            path_sftp = "demographie_ps/input/" + file
            cmd = 'wget --directory-prefix='+dst+' --user="'+username+'" --password="'+password+'"  ftp://'+host+'/'+path_sftp+' --progress=bar'
            subprocess.run(cmd, shell=True)
            print(' --- Commande "'+cmd+'" exécutée')
            print(' --- Nouveau fichier : ', file, ' --> importé')
            print(" -------------------------------------------------------------------------- ")
            print(" ")
    return


# def get_filenames_from_sftp(sftp):
def get_sftp_gpg_filenames(sftp, path_sftp) :
    """
    Fonction permettant de récupérer le nom des 
    fichiers .csv présents au sein du sftp

    Returns:
        filenames[list]: Liste contenant le nom des fichiers .csv présents
                         dans le sftp.
    """
    # Récupération des noms des fichiers sources
    # print(path_sftp)
    # directory_structure ! sftp.listdir_attr('demographie_ps/input')
    directory_structure = sftp.listdir_attr(path_sftp)
    dict_filenames = [attr.filename for attr in directory_structure]
    filenames=[]
    # Boucle permettant de ne récupérer que les fichiers .csv
    for elem in dict_filenames:
        if elem[-4::]=='.gpg':
            filenames.append(elem) 
    return filenames


def delete_old_gpg_files_in_os(sftp, path_os, path_sftp):
    """
    Récupère noms des fichiers déjà importés dans data/input.
    Effectue la comparaison entre les fichiers déjà présent dans data/input et ceux à importer (files_from_sftp)
    Supprime les fichiers présents dans data/input identiques aux fichiers présents dans files_from_sftp et qui vont être à nouveau importés.

    Args:
        files_from_sftp ([List]): Liste contenant le nom des fichiers .csv présents dans le sftp et récupérée via get_filenames_from_os().
        sftp : Elements de connexion au SFTP
        path_os : Direction du dossier OS où vérifier la présence ou non des fichiers à importer.
        path_sftp : Direction du dossier SFTP depuis lequel les fichiers vont être importés.
    """
    # Récupération du nom des fichiers présents dans data/input
    files_in_os = get_os_gpg_filenames(path = path_os)
    print(" --- files_in_os :", files_in_os)
    files_in_sftp = get_sftp_gpg_filenames(sftp = sftp, path_sftp = path_sftp) 
    print(" --- files_in_sftp :", files_in_sftp)    

    # Ci dessous, ancienne version de la fonction à supprimer une fois testée
    """
    dict_filenames_from_os = os.listdir('data/input')
    files_from_os=[]
    
    # Boucle permettant de ne récupérer que les fichiers .csv
    for elem in dict_filenames_from_os:
        if elem[-4::]=='.csv':
            files_from_os.append(elem)
    """

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
            os.remove(path_os + "/" + file)
            print(" --- Ancien fichier :", file, "--> supprimé")
    else:
        print(" --- Aucun fichier existant au sein du dossier data/input de l'os")


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

# Fonction pour récupérer nom des fichiers .gpg
def get_os_gpg_filenames(path):
    dict_filenames = os.listdir(path)
    files = []

    for elem in dict_filenames:
        if elem[-4::]=='.gpg':
            files.append(elem)

    return files


# Fonction pour récupérer nom des fichiers .csv
def get_os_csv_filenames(path_in):
    """
    Fonction permettant de récupérer le nom des fichiers .csv
    situés au sein d'un fichier "path_in" de l'os.
    """
    dict_filenames = os.listdir(path_in)
    files = []

    for elem in dict_filenames:
        if elem[-4::]=='.csv':
            files.append(elem)

    return files


# Fonction de suppression des anciens fichiers transférés vers SFTP
def delete_old_files_in_sftp(server_in_config, path_sftp, path_os):
    """
    Fonction permettant de supprimer les fichiers présents dans le dossier demographie_ps/output du SFTP
    et dont une nouvelle version est sur le point d'être transférée.
    """
    host = server_in_config["host"]
    username = server_in_config["username"]
    password = server_in_config["password"]
    
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(host=host, username=username, password=password, port = 2222, cnopts=cnopts)

    files_in_sftp = get_filenames_from_sftp(sftp, path_sftp)
    
    files_in_os = get_os_csv_filenames(path_os)
    
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


# Test export_sftp
def execute_upload(server_in_config, path_in, path_out):
    """
    Fonction permettant de transférer un fichier présent dans le dossier "path_in" de l'os, 
    vers le dossier "path_out" du SFTP.
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
    ftpResponseMessage = ftp.cwd(path_out)
    print(" --- Dossier cible :", path_out,"->", ftpResponseMessage)
    print(" ")

    # Suppression des fichiers csv présents dans demographie_ps/output du SFTP et identiques aux fichiers qui vont être exportés vers ce même SFTP
    print(" --- Suppression des fichiers au sein du SFTP ---")
    delete_old_files_in_sftp(server_in_config = server_in_config, path_sftp = path_out, path_os = path_in)

    # Récupération des noms des fichiers .csv présent dans data/output à transférer vers SFTP
    files_to_sftp = get_os_csv_filenames(path_in)
    
    # Boucle permettant d'exporter un à un les fichiers présents dans data/output vers le SFTP
    for files in files_to_sftp:
        print(" ")
        print(" ----------------------------------------------- ")
        print(" --- Transfert de", files, "vers le SFTP ---")
        print(" ----------------------------------------------- ")
        
        filepath = path_in + "/" + files
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

