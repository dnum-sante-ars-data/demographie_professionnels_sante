# Modules
import sqlite3
import json
import os
import pandas as pd

from modules import route_sftp
from .query_sqlite import query_create_table, list_table_name
#from route_sqlite import *
#import route_sqlite 
#import requetes_sql_route_sqlite

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



# Lecture du paramétrage
def read_create_table(path_in, table_name):
    table_name = table_name.upper()
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["CREATE TABLE"]
    query_create_table = {}
    for table in L_ret :
        if table["table_name"] == table_name :
            query_create_table = table.copy()
    print("Lecture configuration serveur " + path_in + ".")
    return query_create_table



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


# Initialisation du schéma de la BDD
def init_empty_schema(database = "database", verbose = True):
    """
    Fonction permettant d'initialiser la base de données et de 
    créer les tables nécessaires.
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
    cursor.executescript("""
    DROP INDEX IF EXISTS ACTIVITE_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS ACTIVITE_IDENTIFIANT_DE_L_ACTIVITE;
    DROP INDEX IF EXISTS AUTORISATION_EXERCICE_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS COORDONNEES_ACTIVITE_IDENTIFIANT_DE_L_ACTIVITE;
    DROP INDEX IF EXISTS COORDONNEES_CORRESPONDANCE_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS COORDONNEES_STRUCTURE_IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE;
    DROP INDEX IF EXISTS DIPLOME_OBTENU_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS ETAT_CIVIL_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS EXERCICE_PROFESSIONNEL_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS INSCRIPTION_ORDRE_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS PERSONNE_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS SAVOIR_FAIRE_IDENTIFIANT_PP;
    DROP INDEX IF EXISTS STRUCTURE_ACTIVITE_IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE;
    """)
    cursor.close()
    conn.commit()
    conn.close()

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
    conn = sqlite3.connect(
        database = database
    )
    cursor = conn.cursor()
    cursor.executescript("""
    CREATE INDEX ACTIVITE_IDENTIFIANT_PP on ACTIVITE(IDENTIFIANT_PP);
    CREATE INDEX ACTIVITE_IDENTIFIANT_DE_L_ACTIVITE on ACTIVITE(IDENTIFIANT_DE_L_ACTIVITE);
    CREATE INDEX AUTORISATION_EXERCICE_IDENTIFIANT_PP on AUTEXERC(IDENTIFIANT_PP);
    CREATE INDEX COORDONNEES_ACTIVITE_IDENTIFIANT_DE_L_ACTIVITE on COORDACT(IDENTIFIANT_DE_L_ACTIVITE);
    CREATE INDEX COORDONNEES_CORRESPONDANCE_IDENTIFIANT_PP on COORDCORRESP(IDENTIFIANT_PP);
    CREATE INDEX COORDONNEES_STRUCTURE_IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE on COORDSTRUCT(IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE);
    CREATE INDEX DIPLOME_OBTENU_IDENTIFIANT_PP on DIPLOBT(IDENTIFIANT_PP);
    CREATE INDEX ETAT_CIVIL_IDENTIFIANT_PP on ETATCIV(IDENTIFIANT_PP);
    CREATE INDEX EXERCICE_PROFESSIONNEL_IDENTIFIANT_PP on EXERCPRO(IDENTIFIANT_PP);
    CREATE INDEX INSCRIPTION_ORDRE_IDENTIFIANT_PP on REFERAE(IDENTIFIANT_PP);
    CREATE INDEX PERSONNE_IDENTIFIANT_PP on PERSONNE(IDENTIFIANT_PP);
    CREATE INDEX SAVOIR_FAIRE_IDENTIFIANT_PP on SAVOIRFAIRE(IDENTIFIANT_PP);
    CREATE INDEX STRUCTURE_ACTIVITE_IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE on STRUCTURE(IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE);
    """)
    cursor.close()
    conn.commit()
    conn.close()


# Récupération du nom des colonnes et de la table à compléter
def get_column_and_table_names_for_insee(files):
    """
    Fonction appelée dans insert_data_from_insee afin de récupérer le nom de la table et des colonnes   
    cible.
    """
    if files[:8] == "communes":
        table_name = "INSEE_COMMUNES"
        column_names = (
        'TYPECOM',
        'COM',
        'REG',
        'DEP',
        'ARR',
        'TNCC',
        'NCC',
        'NCCENR',
        'LIBELLE',
        'CAN',
        'COMPARENT'
        )
    elif files[:11] == "departement":
        table_name = "INSEE_DEPARTEMENT"
        column_names = (
        'DEP',
        'REG',
        'CHEFLIEU',
        'TNCC',
        'NCC',
        'NCCENR',
        'LIBELLE'        
        )
    elif files[:6] == "region":
        table_name = "INSEE_REGION"
        column_names = (
        'REG',
        'CHEFLIEU',
        'TNCC',
        'NCC',
        'NCCENR',
        'LIBELLE'
        )

    return column_names, table_name


def insert_data_from_insee(conn, path_insee, verbose = True):
    """
    Fonction appelée par insert_data() et permettant d'importer 
    uniquement les fichiers de l'INSEE dans les tables correspondantes
    """
    # Récupération du nom des fichiers INSEE
    #filenames_from_insee = get_filenames_from_insee()
    filenames_from_insee = route_sftp.get_filenames_from_os(path_insee)[1]     

    # Boucle permettant d'importer les données de chaque fichier INSEE dans la BDD
    for files in filenames_from_insee:
        print(" ----------------------------------------------------------------------------------------------------- ")
        print(" --- Insertion des données depuis :", files.upper(), " --- ")
        print(" ----------------------------------------------------------------------------------------------------- ")      
        # Récupération du chemin où sont stockés les fichiers
        #filepath = "utils/" + files
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


# Récupération du nom des colonnes
def get_column_and_table_names_for_source_files(files):
    """
    Fonction appelée dans insert_data_from_source_files afin de récupérer le nom de la table et des colonnes 
    cible.
    """
    file_name = files[24:-4:].upper()

    if file_name == "PERSONNE":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP', 
        'IDENTIFIANT_PP', 
        'IDENTIFICATION_NATIONALE_PP', 
        'CODE_CIVILITE', 
        'LIBELLE_CIVILITE', 
        'NOM_D_USAGE', 
        'PRENOM_D_USAGE', 
        'NATURE',
        'DATE_D_EFFET', 
        'DATE_DE_MISE_A_JOUR_PERSONNE')
    elif file_name == "AUTEXERC":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP', 
        'IDENTIFIANT_PP', 
        'IDENTIFICATION_NATIONALE_PP', 
        'DATE_EFFET_AUTORISATION', 
        'CODE_TYPE_AUTORISATION', 
        'LIBELLE_TYPE_AUTORISATION', 
        'DATE_FIN_AUTORISATION', 
        'DATE_DE_MISE_A_JOUR_AUTORISATION', 
        'CODE_DISCIPLINE_AUTORISATION', 
        'LIBELLE_DISCIPLINE_AUTORISATION', 
        'CODE_PROFESSION', 
        'LIBELLE_PROFESSION',
        'UNNAMED'
        )
    elif file_name == "ACTIVITE":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP', 
        'IDENTIFIANT_PP', 
        'IDENTIFIANT_DE_L_ACTIVITE', 
        'IDENTIFICATION_NATIONALE_PP', 
        'IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE', 
        'CODE_FONCTION', 
        'LIBELLE_FONCTION', 
        'CODE_MODE_EXERCICE', 
        'LIBELLE_MODE_EXERCICE', 
        'DATE_DE_DEBUT_ACTIVITE', 
        'DATE_DE_FIN_ACTIVITE', 
        'DATE_DE_MISE_A_JOUR_ACTIVITE', 
        'CODE_REGION_EXERCICE', 
        'LIBELLE_REGION_EXERCICE', 
        'CODE_GENRE_ACTIVITE', 
        'LIBELLE_GENRE_ACTIVITE', 
        'CODE_MOTIF_DE_FIN_D_ACTIVITE', 
        'LIBELLE_MOTIF_DE_FIN_D_ACTIVITE', 
        'CODE_SECTION_TABLEAU_PHARMACIENS', 
        'LIBELLE_SECTION_TABLEAU_PHARMACIENS', 
        'CODE_SOUS_SECTION_TABLEAU_PHARMACIENS', 
        'LIBELLE_SOUS_SECTION_TABLEAU_PHARMACIENS', 
        'CODE_TYPE_ACTIVITE_LIBERALE', 
        'LIBELLE_TYPE_ACTIVITE_LIBERALE', 
        'CODE_STATUT_DES_PS_DU_SSA', 
        'LIBELLE_STATUT_DES_PS_DU_SSA', 
        'CODE_STATUT_HOSPITALIER', 
        'LIBELLE_STATUT_HOSPITALIER', 
        'CODE_PROFESSION', 
        'LIBELLE_PROFESSION', 
        'CODE_CATEGORIE_PROFESSIONNELLE', 
        'LIBELLE_CATEGORIE_PROFESSIONNELLE',
        'UNNAMED'
        )
    elif file_name == "COORDACT":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP', 
        'IDENTIFIANT_PP', 
        'IDENTIFIANT_DE_L_ACTIVITE', 
        'IDENTIFICATION_NATIONALE_PP', 
        'IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE', 
        'CODE_PROFESSION', 
        'LIBELLE_PROFESSION', 
        'CODE_CATEGORIE_PROFESSIONNELLE', 
        'LIBELLE_CATEGORIE_PROFESSIONNELLE', 
        'COMPLEMENT_DESTINATAIRE_COORD_ACTIVITE', 
        'COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_ACTIVITE', 
        'NUMERO_VOIE_COORD_ACTIVITE', 
        'INDICE_REPETITION_VOIE_COORD_ACTIVITE', 
        'CODE_TYPE_DE_VOIE_COORD_ACTIVITE', 
        'LIBELLE_TYPE_DE_VOIE_COORD_ACTIVITE', 
        'LIBELLE_VOIE_COORD_ACTIVITE', 
        'MENTION_DISTRIBUTION_COORD_ACTIVITE', 
        'BUREAU_CEDEX_COORD_ACTIVITE', 
        'CODE_POSTAL_COORD_ACTIVITE', 
        'CODE_COMMUNE_COORD_ACTIVITE', 
        'LIBELLE_COMMUNE_COORD_ACTIVITE', 
        'CODE_PAYS_COORD_ACTIVITE', 
        'LIBELLE_PAYS_COORD_ACTIVITE', 
        'TELEPHONE_COORD_ACTIVITE', 
        'TELEPHONE_2_COORD_ACTIVITE', 
        'TELECOPIE_COORD_ACTIVITE', 
        'ADRESSE_EMAIL_COORD_ACTIVITE', 
        'DATE_DE_MISE_A_JOUR_COORD_ACTIVITE', 
        'DATE_DE_FIN_COORD_ACTIVITE',
        'UNNAMED'
        )
    elif file_name == "COORDCORRESP":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP',
        'IDENTIFIANT_PP',
        'IDENTIFICATION_NATIONALE_PP',
        'COMPLEMENT_DESTINATAIRE_COORD_CORRESPONDANCE',
        'COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_CORRESPONDANCE',
        'NUMERO_VOIE_COORD_CORRESPONDANCE',
        'INDICE_REPETITION_VOIE_COORD_CORRESPONDANCE',
        'CODE_TYPE_DE_VOIE_COORD_CORRESPONDANCE',
        'LIBELLE_TYPE_DE_VOIE_COORD_CORRESPONDANCE',
        'LIBELLE_VOIE_COORD_CORRESPONDANCE',
        'MENTION_DISTRIBUTION_COORD_CORRESPONDANCE',
        'BUREAU_CEDEX_COORD_CORRESPONDANCE',
        'CODE_POSTAL_COORD_CORRESPONDANCE',
        'CODE_COMMUNE_COORD_CORRESPONDANCE',
        'LIBELLE_COMMUNE_COORD_CORRESPONDANCE',
        'CODE_PAYS_COORD_CORRESPONDANCE',
        'LIBELLE_PAYS_COORD_CORRESPONDANCE',
        'TELEPHONE_COORD_CORRESPONDANCE',
        'TELEPHONE_2_COORD_CORRESPONDANCE',
        'TELECOPIE_COORD_CORRESPONDANCE',
        'ADRESSE_EMAIL_COORD_CORRESPONDANCE',
        'DATE_DE_MISE_A_JOUR_COORD_CORRESPONDANCE',
        'DATE_DE_FIN_COORD_CORRESPONDANCE',
        'UNNAMED'
        )
    elif file_name == "COORDSTRUCT":
        column_names = (
        'IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE',    
        'COMPLEMENT_DESTINATAIRE_COORD_STRUCTURE',      
        'COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_STRUCTURE',
        'NUMERO_VOIE_COORD_STRUCTURE',           
        'INDICE_REPETITION_VOIE_COORD_STRUCTURE',
        'CODE_TYPE_DE_VOIE_COORD_STRUCTURE',   
        'LIBELLE_TYPE_DE_VOIE_COORD_STRUCTURE',
        'LIBELLE_VOIE_COORD_STRUCTURE',
        'MENTION_DISTRIBUTION_COORD_STRUCTURE',
        'BUREAU_CEDEX_COORD_STRUCTURE',
        'CODE_POSTAL_COORD_STRUCTURE',
        'CODE_COMMUNE_COORD_STRUCTURE',
        'LIBELLE_COMMUNE_COORD_STRUCTURE',
        'CODE_PAYS_COORD_STRUCTURE',
        'LIBELLE_PAYS_COORD_STRUCTURE',
        'TELEPHONE_COORD_STRUCTURE',
        'TELEPHONE_2_COORD_STRUCTURE',
        'TELECOPIE_COORD_STRUCTURE',
        'ADRESSE_EMAIL_COORD_STRUCTURE',
        'DATE_DE_MISE_A_JOUR_COORD_STRUCTURE',
        'DATE_DE_FIN_COORD_STRUCTURE',
        'UNNAMED'
        )
    elif file_name == "DIPLOBT":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP',
        'IDENTIFIANT_PP',
        'IDENTIFICATION_NATIONALE_PP',
        'CODE_TYPE_DIPLOME_OBTENU',
        'LIBELLE_TYPE_DIPLOME_OBTENU',
        'CODE_DIPLOME_OBTENU', 
        'LIBELLE_DIPLOME_OBTENU',
        'DATE_DE_MISE_A_JOUR_DIPLOME_OBTENU',
        'CODE_LIEU_OBTENTION',
        'LIBELLE_LIEU_OBTENTION',
        'DATE_D_OBTENTION_DIPLOME',
        'NUMERO_DIPLOME',
        'UNNAMED'
        )
    elif file_name == "ETATCIV":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP',
        'IDENTIFIANT_PP',
        'IDENTIFICATION_NATIONALE_PP',
        'CODE_STATUT_ETAT_CIVIL',
        'LIBELLE_STATUT_ETAT_CIVIL',
        'CODE_SEXE',
        'LIBELLE_SEXE',
        'NOM_DE_FAMILLE',
        'PRENOMS',
        'DATE_DE_NAISSANCE',
        'LIEU_DE_NAISSANCE',
        'DATE_DE_DECES',
        'DATE_D_EFFET_DE_L_ETAT_CIVIL',
        'CODE_COMMUNE_DE_NAISSANCE',
        'LIBELLE_COMMUNE_DE_NAISSANCE',
        'CODE_PAYS_DE_NAISSANCE',
        'LIBELLE_PAYS_DE_NAISSANCE',
        'DATE_DE_MISE_A_JOUR_ETAT_CIVIL',
        'UNNAMED'
        )
    elif file_name == "EXERCPRO":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP',
        'IDENTIFIANT_PP',
        'IDENTIFICATION_NATIONALE_PP',
        'CODE_CIVILITE_D_EXERCICE',
        'LIBELLE_CIVILITE_D_EXERCICE',
        'NOM_D_EXERCICE',
        'PRENOM_D_EXERCICE',
        'CODE_PROFESSION',
        'LIBELLE_PROFESSION',
        'CODE_CATEGORIE_PROFESSIONNELLE',
        'LIBELLE_CATEGORIE_PROFESSIONNELLE',
        'DATE_DE_FIN_EXERCICE',
        'DATE_DE_MISE_A_JOUR_EXERCICE',
        'DATE_EFFET_EXERCICE',
        'CODE_AE_1E_INSCRIPTION',
        'LIBELLE_AE_1E_INSCRIPTION',
        'DATE_DEBUT_1E_INSCRIPTION',
        'DEPARTEMENT_1E_INSCRIPTION',
        'LIBELLE_DEPARTEMENT_1E_INSCRIPTION',
        'UNNAMED'
        )
    elif file_name == "REFERAE":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP',
        'IDENTIFIANT_PP',
        'IDENTIFICATION_NATIONALE_PP',
        'CODE_AE',
        'LIBELLE_AE',
        'DATE_DEBUT_INSCRIPTION',
        'DATE_FIN_INSCRIPTION',
        'DATE_DE_MISE_A_JOUR_INSCRIPTION',
        'CODE_STATUT_INSCRIPTION',
        'LIBELLE_STATUT_INSCRIPTION',
        'CODE_DEPARTEMENT_INSCRIPTION',
        'LIBELLE_DEPARTEMENT_INSCRIPTION',
        'CODE_DEPARTEMENT_ACCUEIL',
        'LIBELLE_DEPARTEMENT_ACCUEIL',
        'CODE_PROFESSION',
        'LIBELLE_PROFESSION',
        'CODE_CATEGORIE_PROFESSIONNELLE',
        'LIBELLE_CATEGORIE_PROFESSIONNELLE',
        'UNNAMED'
        )
    elif file_name == "SAVOIRFAIRE":
        column_names = (
        'TYPE_D_IDENTIFIANT_PP',
        'IDENTIFIANT_PP',
        'IDENTIFICATION_NATIONALE_PP',
        'CODE_SAVOIR_FAIRE',
        'LIBELLE_SAVOIR_FAIRE',
        'CODE_TYPE_SAVOIR_FAIRE',
        'LIBELLE_TYPE_SAVOIR_FAIRE',
        'CODE_PROFESSION',
        'LIBELLE_PROFESSION',
        'CODE_CATEGORIE_PROFESSIONNELLE',
        'LIBELLE_CATEGORIE_PROFESSIONNELLE',
        'DATE_RECONNAISSANCE_SAVOIR_FAIRE',
        'DATE_DE_MISE_A_JOUR_SAVOIR_FAIRE',
        'DATE_ABANDON_SAVOIR_FAIRE',
        'UNNAMED'
        )
    elif file_name == "STRUCTURE":
        column_names = (
        'TYPE_DE_STRUCTURE',
        'IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE',
        'IDENTIFICATION_NATIONALE_DE_LA_STRUCTURE',
        'NUMERO_SIRET',
        'NUMERO_SIREN',
        'NUMERO_FINESS_ETABLISSEMENT',
        'NUMERO_FINESS_EJ',
        'RPPS_RANG',
        'ADELI_RANG',
        'NUMERO_LICENCE_OFFICINE',
        'DATE_D_OUVERTURE_STRUCTURE',
        'DATE_DE_FERMETURE_STRUCTURE',
        'DATE_DE_MISE_A_JOUR_STRUCTURE',
        'CODE_APE',
        'LIBELLE_APE',
        'CODE_CATEGORIE_JURIDIQUE',
        'LIBELLE_CATEGORIE_JURIDIQUE',
        'CODE_SECTEUR_D_ACTIVITE',
        'LIBELLE_SECTEUR_D_ACTIVITE',
        'RAISON_SOCIALE',
        'ENSEIGNE_COMMERCIALE',
        'UNNAMED'
        )

    return column_names, file_name


def insert_data_from_source_files(conn, path_os_input, verbose = True):
    """
    Fonction appelée par insert_data() et permettant d'importer 
    uniquement les fichiers sources dans les tables correspondantes
    """
    #Récupération du nom des fichiers sources    
    #filenames_from_os = get_filenames_from_source_files(path_os_input)
    #filenames_from_os = ["Extraction_RPPS_Profil1_Personne.csv"]
    #print(" --- filenames_from_os : ", filenames_from_os)
    filenames_from_os = route_sftp.get_filenames_from_os(path_os_input)[1]
    print(" ")

    for files in filenames_from_os:
        print(" ------------------------------------------------------------------------------------ ")
        print(" --- Insertion des données depuis : ", files.upper(), "--- ")
        print(" ------------------------------------------------------------------------------------ ")       

        # Récupération du chemin où sont stockés les fichiers csv
        #filepath = "data/input/" + files
        filepath = path_os_input + files

        # Récupération du nom des colonnes et de la table en fonction du fichier traité
        column_names, table_name  = get_column_and_table_names_for_source_files(files)
        #print("Test column_names : ", column_names)

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


