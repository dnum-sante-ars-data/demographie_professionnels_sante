from fileinput import filename
import json
import logging
import pysftp

import subprocess
import wget

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

# telechargement complet via wget
def save_wget_sftp(server_in_config):
    """Telechargement complet via wget

    Args:
        server_in_config ([dict]): Configuration du server au 
        format dictionnaire. Récupéré via read_config_sftp
    """
    print('--- Lancement de la commande wget')
    #sftp_host = sftp_host
    host = server_in_config["host"]
    #print('host :', host)
    username = server_in_config["username"]
    #print('username :', username)
    password = server_in_config["password"]
    #print('password :', password)

    # localisation du fichier a recuperer sur le serveur sftp
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        # Récupération des fichiers à importer
        filenames_from_sftp = get_filenames_from_sftp(sftp)
        print(" -- Fichiers à importer depuis sftp : ", filenames_from_sftp)
        
        # Suppression des anciens fichiers importés précédemment 
        delete_old_files(filenames_from_sftp)

        # Boucle for pour importer nouveaux fichiers depuis sftp
        for file in filenames_from_sftp:
            dst = "data/input"
            path_sftp = "demographie_ps/" + file
            cmd = 'wget --directory-prefix='+dst+' --user="'+username+'" --password="'+password+'"  ftp://'+host+'/'+path_sftp+' --progress=bar'
            subprocess.run(cmd, shell=True)
            print(' - Commande "'+cmd+'" exécutée')
            print(' -- Nouveau fichier : ', file, ' --> importé')
    return


def get_filenames_from_sftp(sftp) :
    """
    Fonction permettant de récupérer le nom des 
    fichiers .csv présents au sein du sftp

    Returns:
        filenames[list]: Liste contenant le nom des fichiers .csv présents
                         dans le sftp.
    """
    # Récupération des noms des fichiers sources
    directory_structure = sftp.listdir_attr('demographie_ps/')
    dict_filenames = [attr.filename for attr in directory_structure]
    filenames=[]
    # Boucle permettant de ne récupérer que les fichiers .csv
    for elem in dict_filenames:
        if elem[-4::]=='.csv':
            filenames.append(elem) 
    return filenames


def delete_old_files(files_from_sftp):
    """
    Récupère noms des fichiers déjà importés dans data/input.
    Effectue la comparaison entre les fichiers déjà présent dans data/input et ceux à importer (files_from_sftp)
    Supprime les fichiers présents dans data/input identiques aux fichiers présents dans files_from_sftp et qui vont être à nouveau importés.

    Args:
        files_from_sftp ([List]): Liste contenant le nom des fichiers .csv présents dans le sftp et récupérée via get_filenames_from_os().
    """
    # Récupération du nom des fichiers présents dans data/input
    dict_filenames_from_os = os.listdir('data/input')
    files_from_os=[]
    
    # Boucle permettant de ne récupérer que les fichiers .csv
    for elem in dict_filenames_from_os:
        if elem[-4::]=='.csv':
            files_from_os.append(elem)

    # Comparaison entre fichiers présents dans data/input et ceux dans sftp
    filenames_to_delete = set(files_from_sftp) & set(files_from_os)
    
    # Suppression des fichiers présents dans data/input et qui vont être à nouveau importés
    # depuis sftp
    if len(filenames_to_delete) != 0:
        print(' -- Fichiers à supprimer : ', filenames_to_delete)
        for file in filenames_to_delete:
            os.remove("data/input/"+file)
            print(" -- Ancien fichier : ", file, " --> supprimé")
