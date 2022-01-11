# Modules
import argparse
import logging
from datetime import datetime

# Modules personnalisés
from modules import route_sqlite

# Commandes
def __main__(args):
    if args.commande == "init_db":
        exe_db_init()
    return

# Exécution de l'initialisation de la BDD
def exe_db_init():
<<<<<<< HEAD
    
=======
    print(" - Deploiement ...")
    param_config = route_sqlite.read_config_db("settings/settings.json", server="LOCAL SERVER")
    route_sqlite.deploy_database(database=param_config["database"])
>>>>>>> b1e3ace8f433539081040ab19f45fbb427e6160e
    return

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
<<<<<<< HEAD
    __main__(args)
=======
    __main__(args)
>>>>>>> b1e3ace8f433539081040ab19f45fbb427e6160e
