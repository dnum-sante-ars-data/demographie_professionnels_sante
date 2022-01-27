# -*-coding:Latin-1 -*

# Modules
import argparse
import logging
from datetime import datetime
from tabnanny import verbose


# Modules personnalisés
from modules import route_sftp, route_sqlite, private_transform


# Commandes
def __main__(args):
    if args.commande == "import":
        import_wget_sftp()
    elif args.commande == "init_database":
        exe_db_init()
    elif args.commande == "transform":
        transform()
    return


# Fonction d'import des fichiers depuis SFTP vers data/input
def import_wget_sftp():
    param_config = route_sftp.read_config_sftp("settings/settings.json", server_name="ATLASANTE SFTP DEPOT")
    print(param_config)
    route_sftp.save_wget_sftp(param_config)


# Exécution de l'initialisation de la BDD
def exe_db_init():
    print(" -- Deploiement -- ")
    param_config = route_sqlite.read_config_db("settings/settings.json", server="LOCAL SERVER")
    route_sqlite.deploy_database(database=param_config["database"])
    print(" -- Transformation -- ")
    route_sqlite.init_empty_schema(database = param_config["database"], verbose = True)
    route_sqlite.insert_data(database = param_config["database"], verbose = True)
    return


# Fonction de transformation
def transform():
    print(" - Transformation - ")
    param_config = route_sqlite.read_config_db("settings/settings.json", server = "LOCAL SERVER")
    # Remise en forme des données personne
    #private_transform.transform_ods_personne(database = param_config["database"], verbose = True)
    # Remise en forme des données activité
    private_transform.transform_ods_activite(database = param_config["database"], verbose = True)
    print(" - Transformation de ODS_PERSONNE et ODS_ACTIVITE réalisées")
    # Transformation sur les  référentiels géo pour constituer les tables de dimension
    # private_transform.transform_corresp_cp(database = param_config["database"], verbose = True)
    # Création des csv pour les visualisations
    private_transform.transform_to_csv(database = param_config["database"], verbose = True)
    print(" - Transformations terminées")




# Initialisation du parsing
parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
args = parser.parse_args()

# # Genération des logs
# logging.basicConfig(filename="log/log_debug.log",
#                             filemode='a',
#                             format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                             datefmt='%Y-%m-%d %H:%M:%S',
#                             level=logging.DEBUG)
# logging.basicConfig(filename="log/log_info.log",
#                             filemode='a',
#                             format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                             datefmt='%Y-%m-%d %H:%M:%S',
#                             level=logging.INFO)
# logging.basicConfig(filename="log/log_warning.log",
#                             filemode='a',
#                             format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#                             datefmt='%Y-%m-%d %H:%M:%S',
#                             level=logging.WARNING)

# logging.info("Utilisation de l'utilitaire.")

# Core
if __name__ == "__main__":
    __main__(args)
