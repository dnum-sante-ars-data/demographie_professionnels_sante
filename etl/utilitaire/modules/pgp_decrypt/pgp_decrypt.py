# Modules
import sqlite3
import json
import gnupg
import os


# Lecture du paramétrage
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

<<<<<<< HEAD

def decrypt_file(path):
    gpg = gnupg.GPG("/usr/bin/gpg")
    file = "Extraction_RPPS_Profil1_SavoirFaire.csv.gpg"
    
    print(" --- path :", path)
    print(" --- file :", file)
    
    x = path + "/" + file 
    print(" --- x :", x)   

    with open(x, "rb") as f:
        print(" --- f :", f)
        status = gpg.decrypt_file(f, passphrase = "01062021@beD", output = x)

    print(status.ok)
    print(status.stderr)

    """
# Décriptage des fichiers dans sources
def decrypt_file(path):
    gpg = gnupg.GPG("/usr/bin/gpg")
    #path = "data/input"
    
    #dict_filenames = os.listdir(path)
    
    file = "Extraction_RPPS_Profil1_EtatCiv.csv.gpg"   
    with open(path + "/" + file, "rb") as f:
        print(" --- f :", f)
        status = gpg.decrypt_file(f, passphrase = "01062021@beD", output = path + "/" + file)

    print(status.ok)
    print(status.stderr)

<<<<<<< Updated upstream
=======
=======

def decrypt_file(path):
    gpg = gnupg.GPG("/usr/bin/gpg")
    file = "Extraction_RPPS_Profil1_SavoirFaire.csv.gpg"
    
    print(" --- path :", path)
    print(" --- file :", file)
    
    x = path + "/" + file 
    print(" --- x :", x)   

    with open(x, "rb") as f:
        print(" --- f :", f)
        status = gpg.decrypt_file(f, passphrase = "01062021@beD", output = x)

    print(status.ok)
    print(status.stderr)

    """
# Décriptage des fichiers dans sources
def decrypt_file(path):
    gpg = gnupg.GPG("/usr/bin/gpg")
    #path = "data/input"
    
    #dict_filenames = os.listdir(path)
    
    file = "Extraction_RPPS_Profil1_EtatCiv.csv.gpg"   
    with open(path + "/" + file, "rb") as f:
        print(" --- f :", f)
        status = gpg.decrypt_file(f, passphrase = "01062021@beD", output = path + "/" + file)

    print(status.ok)
    print(status.stderr)

>>>>>>> Stashed changes
    
    for elem in dict_filenames:
        file = elem
        print(" --- File :", elem)
        with open(path + "/" + file, "rb") as f:
            status = gpg.decrypt_file(f, passphrase = "01062021@beD", output = path + "/" + file)
    
        print(" --- Status ok :", status.ok)
        print(" --- Status stderr :", status.stderr)
    """
<<<<<<< Updated upstream
=======
>>>>>>> 5991021c26a050c6b9e3f246a5e00330f3716b46
>>>>>>> Stashed changes
