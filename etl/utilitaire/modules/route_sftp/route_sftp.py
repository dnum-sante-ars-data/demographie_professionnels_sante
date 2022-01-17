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
    print('host :', host)
    username = server_in_config["username"]
    print('username :', username)
    password = server_in_config["password"]
    print('password :', password)
    # localisation du fichier a recuperer sur le serveur sftp
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None 
    with pysftp.Connection(host=host, username=username, password=password, port =2222, cnopts=cnopts) as sftp:
        dst = "data/imput"
        path_sftp = "demographie_ps/RPPS_Profil1_CoordCorresp"
        cmd = 'wget --directory-prefix='+dst+' --user="'+username+'" --password="'+password+'"  ftp://'+host+'/'+path_sftp+' --progress=bar'
        subprocess.run(cmd, shell=True)
        print(' - Commande "'+cmd+'" exécutée')
    return

