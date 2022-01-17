# Modules
import argparse
import logging
from datetime import datetime
from modules.route_sftp import route_sftp

# Modules personnalisés
from modules import route_sqlite

# Commandes
def __main__(args):
    if args.commande == "init_db":
        exe_db_init()
    elif args.commande == "import":
        import_wget_sftp()
    return

# Exécution de l'initialisation de la BDD
def exe_db_init():
    print(" - Deploiement ...")
    param_config = route_sqlite.read_config_db("settings/settings.json", server="LOCAL SERVER")
    route_sqlite.deploy_database(database=param_config["database"])
    return

# Fonction d'import des fichiers depuis SFTP
def import_wget_sftp():
    param_config = route_sftp.read_config_sftp("settings/settings.json", server_name="ATLASANTE SFTP DEPOT")
    print(param_config)
    route_sftp.save_wget_sftp(param_config)

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
