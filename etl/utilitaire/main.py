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
    elif args.commande == "transform_export":
        transform_export()
    elif args.commande == "export_sftp":
        export_to_sftp()
    return


# Fonction d'import des fichiers depuis SFTP vers data/input
def import_wget_sftp():
    param_config = route_sftp.read_config_sftp("settings/settings.json", server_name="FTP ODS")
    print(param_config)
    route_sftp.save_wget_sftp(param_config)


# Exécution de l'initialisation de la BDD
def exe_db_init():
    print(" -- Deploiement -- ")
    param_config = route_sqlite.read_config_db("settings/settings.json", server="LOCAL SERVER")
    route_sqlite.deploy_database(database=param_config["database"])
    print(" -- Transformation -- ")
    route_sqlite.init_empty_schema(database = param_config["database"], verbose = True)
    route_sqlite.drop_indexes(database = param_config["database"], verbose = True)
    route_sqlite.insert_data(database = param_config["database"], verbose = True)
    route_sqlite.create_indexes(database = param_config["database"], verbose = True)
    return


# Fonction de transformation et export
def transform_export():
    print(" - Transformation et export - ")
    param_config = route_sqlite.read_config_db("settings/settings.json", server = "LOCAL SERVER")
    private_transform.transform_export(filepath_activites = "data/output/activites.csv", filepath_personnes="data/output/personnes.csv", database = param_config["database"], verbose = True)


# Fonction export vers SFTP
def export_to_sftp():
    print(" - Exportation vers SFTP")
    param_config = route_sftp.read_config_ecriture("settings/settings.json", server_name = "FTP ODS")
    print(param_config)
    route_sftp.execute_upload(param_config)
    print(" --- Fichiers exportés vers SFTP")


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
