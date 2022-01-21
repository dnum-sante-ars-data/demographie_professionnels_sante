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
    if args.commande == "init_database":
        exe_db_init()
    elif args.commande == "import":
        import_wget_sftp()
    return

# Exécution de l'initialisation de la BDD
def exe_db_init():
    print(" -- Deploiement -- ")
    param_config = route_sqlite.read_config_db("settings/settings.json", server="LOCAL SERVER")
    route_sqlite.deploy_database(database=param_config["database"])
    print(" -- Transformation -- ")
    route_sqlite.init_empty_schema(database = param_config["database"], verbose = True)
    route_sqlite.insert_data_2(database = param_config["database"], verbose = True)   
 # route_sqlite.insert_autexerc(path_autexerc = "data/input/Extraction_RPPS_Profil1_AutExerc.csv")
    return

# Fonction d'import des fichiers depuis SFTP
def import_wget_sftp():
    param_config = route_sftp.read_config_sftp("settings/settings.json", server_name="ATLASANTE SFTP DEPOT")
    print(param_config)
    route_sftp.save_wget_sftp(param_config)

# Fonction de transformation
def transform():
    print(" - Transformation - ")
    param_config = route_sqlite.read_config_db("settings/settings.json", server = "LOCAL SERVER")
    # Remise en forme des données démographiques des professionnels de santé
    private_transform.transform_f_libreacces_ps(database = param_config["database"], verbose = True)
    # Remise en forme des données démographiques de population
    private_transform.transform_f_population(database = param_config["database"], verbose = True)
    # Remise en forme de la table ref_atlasante_t_corresp_cp
    private_transform.transform_corresp_cp(database = param_config["database"], verbose = True)
    # Transformation sur les  référentiels géo pour constituer les tables de dimension
    private_transform.transform_corresp_cp(database = param_config["database"], verbose = True)
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
