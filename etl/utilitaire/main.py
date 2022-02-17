# -*-coding:Latin-1 -*

# Modules
import argparse
import logging
from datetime import datetime
from tabnanny import verbose


# Modules personnalisés
from modules import route_sftp, route_sqlite, private_transform, pgp_decrypt, control


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
    elif args.commande == "control":
        control_output()
    elif args.commande == "all":
        all_functions()        
    return


# Fonction d'import des fichiers depuis SFTP vers data/input
def import_wget_sftp():
    print(" ")
    print(" --- Import des fichiers depuis le SFTP --- ")
    param_config = route_sftp.read_config_sftp("settings/settings.json", server_name="FTP ODS")
    param_path_sftp_input = route_sftp.read_path_sftp("settings/settings.json", folder_name="sftp_input")
    param_path_os_input = route_sftp.read_path_os("settings/settings.json", folder_name = "os_input")
    route_sftp.save_wget_sftp(param_config, param_path_os_input["path"], param_path_sftp_input["path"])
    pgp_decrypt.decrypt_file(path = param_path_os_input["path"])


# Exécution de l'initialisation de la BDD
def exe_db_init():
    print(" ")
    print(" --- Deploiement --- ")
    print(" --- Récupération des paramètres :")
    param_config = route_sqlite.read_config_db("settings/settings.json", server="LOCAL SERVER")
    param_path_os_insee = route_sftp.read_path_os("settings/settings.json", folder_name = "os_insee")
    param_path_os_input = route_sftp.read_path_os("settings/settings.json", folder_name = "os_input")
    print(" --- param_config :", param_config)
    print(" --- param_path_os_insee :", param_path_os_insee)
    print(" --- param_path_os_input :", param_path_os_input)
    print(" ")
    route_sqlite.deploy_database(database=param_config["database"])
    print(" ")
    print(" --- Transformation --- ")
    route_sqlite.init_empty_schema(database = param_config["database"], verbose = True)
    route_sqlite.drop_indexes(database = param_config["database"], verbose = True)
    route_sqlite.insert_data(database = param_config["database"], path_insee = param_path_os_insee["path"], path_os = param_path_os_input["path"], verbose = True)
    route_sqlite.create_indexes(database = param_config["database"], verbose = True)
    print(" --- Initialisation de la BDD terminée --- ")
    return


# Fonction de transformation et export
def transform_export():
    print(" ")
    print(" --- Transformation et export --- ")
    param_config = route_sqlite.read_config_db("settings/settings.json", server = "LOCAL SERVER")
    param_path_activites = private_transform.read_filepath("settings/settings.json", file_name = "activites.csv")
    param_path_personnes = private_transform.read_filepath("settings/settings.json", file_name = "personnes.csv")
    private_transform.transform_export(filepath_activites = param_path_activites["path"], filepath_personnes = param_path_personnes["path"], database = param_config["database"], verbose = True)


# Fonction export vers SFTP
def export_to_sftp():
    print(" ")
    print(" --- Exportation vers SFTP --- ")
    print(" --- Récupération des paramètres :")
    param_config = route_sftp.read_config_ecriture("settings/settings.json", server_name = "FTP ODS")
    param_path_sftp_output = route_sftp.read_path_sftp("settings/settings.json", folder_name = "sftp_output")
    param_path_os_output = route_sftp.read_path_os("settings/settings.json", folder_name = "os_output")
    param_path_os_input = route_sftp.read_path_os("settings/settings.json", folder_name = "os_input")
    
    print(" --- param_config :", param_config)
    print(" --- param_path_sftp_output :", param_path_sftp_output)
    print(" --- param_path_os_output :", param_path_os_output)
    print(" --- param_path_os_input :", param_path_os_input)

    # execute_upload permettant d'importer fichiers .csv de data/output vers SFTP
    route_sftp.execute_upload(param_config, path_os = param_path_os_output["path"], path_sftp = param_path_sftp_output["path"])
    # execute_upload permettant d'importer fichiers .csv de data/input vers SFTP
    route_sftp.execute_upload(param_config, path_os = param_path_os_input["path"], path_sftp = param_path_sftp_output["path"])

    print(" --- Fichiers exportés vers SFTP --- ")


def control_output():
    param_path_os_output = route_sftp.read_path_os("settings/settings.json", folder_name 
= "os_output")
    control.test_not_null(param_path_os_output["path"])


def all_functions():
    import_wget_sftp()
    exe_db_init()
    transform_export()
    export_to_sftp()
    control_output()

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
