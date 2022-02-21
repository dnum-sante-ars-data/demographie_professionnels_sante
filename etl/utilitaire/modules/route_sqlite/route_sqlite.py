# Modules
import sqlite3
import json
import os
import pandas as pd

from utils import *
from .query_sqlite import *


# Création de la BDD
def deploy_database(database="database") :
    """
    Déploiement de la database.

    Paramètre : 
        - database : Paramètres de la database à déployer.
    """ 
    conn = sqlite3.connect(
        database=database
    )
    cursor = conn.cursor()
    conn.commit
    conn.close()
    print("Création de la BDD.")
    return


# Initialisation du schéma de la BDD
def init_empty_schema(database = "database", verbose = True):
    """
    Fonction permettant d'initialiser la base de données et de 
    créer les tables nécessaires.
    
    Paramètre :
        - database : Paramètres de la database.
    """
    # Initialisation de la bdd
    print(" --- Initialisation de la BDD --- ")
    conn = sqlite3.connect(database = database)
    cursor = conn.cursor()

    # Création des tables au sein de la bdd
    for table in list_table_name:
        print(" --- table :", table)
        query = query_create_table(table)
        print(" --- query :", query)
        cursor.execute(query)
        print(" --- query de", table,"executée")
        print(" ")

    cursor.close()
    conn.commit
    conn.close
    if verbose :
        print("Initialisation de la BDD terminée")
    return


def drop_indexes(database="database", verbose = True):
    """
    Fonction permettant de supprimer les indexes si existants
    """
    if verbose :
        print(" --- Suppression des index si existants --- ")
    conn = sqlite3.connect(
        database = database
    )
    cursor = conn.cursor()

    query = query_drop_index()
    cursor.executescript(query)
    print(" --- query drop index réalisée")
    cursor.close()
    conn.commit()
    conn.close()


def insert_data_from_source_files(conn, path_os_input, verbose = True):
    """
    Fonction appelée par insert_data() et permettant d'importer 
    uniquement les fichiers sources dans les tables correspondantes.

    Paramètres :
        - conn : Eléments de connexion à la bdd.
        - path_os_input : Chemin du dossier où sont stockés les fichiers sources.
    """
    #Récupération du nom des fichiers sources    
    filenames_from_os = utils.get_filenames_from_os(path_os_input)[1]
    print(" ")

    for files in filenames_from_os:
        print(" ------------------------------------------------------------------------------------ ")
        print(" --- Insertion des données depuis : ", files.upper(), "--- ")
        print(" ------------------------------------------------------------------------------------ ")
        # Récupération du chemin où sont stockés les fichiers csv
        filepath = path_os_input + files

        # Récupération du nom des colonnes et de la table en fonction du fichier traité
        column_names, table_name  = get_column_and_table_names_for_source_files(files)

        # Modification pour remplacer """ par "" et éviter erreurs 
        if files == "Extraction_RPPS_Profil1_DiplObt.csv":
            text = open(filepath, "r")
            text = ''.join([i for i in text]).replace('"""', '""')
            x = open(filepath, "w")
            x.writelines(text)
            x.close()

        # Lecture du fichier csv
        print(" --- Lecture et transformation du fichier :", files)
        insert_file = pd.read_csv(filepath, sep=";", header = 0, names = column_names, dtype="str", low_memory = False)

        print(" --- Nom des colonnes du fichier", files,":", insert_file.columns)

        # Insertion du Dataframe dans la table cible
        print(" --- Insertion des données au sein de la table :", table_name)
        insert_file.to_sql(table_name, conn, if_exists = "replace", index = False)

        if verbose :
            print(' --- Insertion des données depuis', files, 'vers la table', table_name, 'réussie --- ')
            print(" -------------------------------------------------------------------------------------------- ")
            print(" ")


def insert_data_from_insee(conn, path_insee, verbose = True):
    """
    Fonction appelée par insert_data() et permettant d'importer 
    uniquement les fichiers de l'INSEE dans les tables correspondantes

    Paramètres :
        - conn : Elements de connexion à la bdd.
        - path_insee : Chemin du dossier où sont stockés les fichiers de données issus de l'INSEE.     
    """
    # Récupération du nom des fichiers INSEE
    filenames_from_insee = utils.get_filenames_from_os(path_insee)[1]

    # Boucle permettant d'importer les données de chaque fichier INSEE dans la BDD
    for files in filenames_from_insee:
        print(" ----------------------------------------------------------------------------------------------------- ")
        print(" --- Insertion des données depuis :", files.upper(), " --- ")
        print(" ----------------------------------------------------------------------------------------------------- ")
        # Récupération du chemin où sont stockés les fichiers
        filepath = path_insee + files

        # Récupération du nom des colonnes et de la table en fonction du fichier
        column_names, table_name = get_column_and_table_names_for_insee(files)

        # Lecture du fichier csv
        print(" --- Lecture et transformation du fichier :", files)
        insert_file = pd.read_csv(filepath, sep=",", header = 0, names = column_names, dtype="str")    

        print(" --- Nom des colonnes du fichier", files, ":", insert_file.columns)

        print(" --- Insertion des données au sein de la table :", table_name)
        insert_file.to_sql(table_name, conn, if_exists = "replace", index = False)

        if verbose :
            print(" --- Insertion des données depuis le fichier", files, "vers la table", table_name, "réussie ---")
            print(" ----------------------------------------------------------------------------------------------------- ")
            print(" ")


def insert_data(database, path_insee, path_os, verbose = True):
    """
    Fonction permettant d'importer les données depuis les fichiers sources
    et les fichiers de l'insee, vers la base de données demographie_ps.db

    Paramètres : 
        database : Eléments de connexions à la BDD demographie_ps.db.
        path_insee : Chemin du dossier où sont stockés les fichiers INSEE.
        path_os : Chemin du dossier où sont stockés les fichiers sources.
    """
    if verbose :
        print(" --- Insertion des données --- ")
    conn = sqlite3.connect(database = database)
    cursor = conn.cursor()

    # Insertion des données des fichiers sources
    print(" --- Insertion des données depuis fichiers sources --- ")
    print(" ")
    insert_data_from_source_files(conn, path_os)
   
    # Insertion des données des fichiers INSEE
    print(" --- Insertion des données depuis fichiers INSEE --- ")
    print(" ")
    insert_data_from_insee(conn, path_insee)
    return


def create_indexes(database="database", verbose = True):
    """
    Fonction permettant de créer les indexes si existants
    """
    if verbose :
        print(" --- Création des index --- ")
    
    # Connexion à la bdd
    conn = sqlite3.connect(
        database = database
    )
    cursor = conn.cursor()
    
    # Création d'une liste create_index pour stocker les requêtes présentent 
    # dans query_create_index() de query_sqlite.py
    create_index = []
    create_index = query_create_index()
    
    # Execution de la requete pour créer l'index
    for requete in create_index:
        print(" --- Requête create_index :", requete)
        cursor.executescript(requete)

    cursor.close()
    conn.commit()
    conn.close()

