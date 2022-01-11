# Modules
import sqlite3
import json

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

# Création de la BDD
def deploy_database(database="database") :
    conn = sqlite3.connect(
        database=database
    )
    cursor = conn.cursor()
    conn.commit
    conn.close()
    print("Création de la BDD.")
    return

# Création du schéma de dump


# Dump des données
## Données privées


## Données restreintes


# Création du schéma de l'entrepôt de données
