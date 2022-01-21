# Modules
import sqlite3
import json
import os
import pandas as pd

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


# Initialisation du schéma de la BDD
def init_empty_schema(database = "database", verbose = True):
    print(" --- Initialisation de la BDD --- ")
    conn = sqlite3.connect(database = database)
    cursor = conn.cursor()
    query_create_autexerc = """
    CREATE TABLE IF NOT EXISTS AUTEXERC (
        TYPE_D_IDENTIFIANT_PP            TEXT, 
        IDENTIFIANT_PP                   TEXT,
        IDENTIFICATION_NATIONALE_PP      TEXT,
        DATE_EFFET_AUTORISATION          TEXT,
        CODE_TYPE_AUTORISATION           TEXT,
        LIBELLE_TYPE_AUTORISATION        TEXT,
        DATE_FIN_AUTORISATION            TEXT,
        DATE_DE_MISE_A_JOUR_AUTORISATION TEXT,
        CODE_DISCIPLINE_AUTORISATION     TEXT,
        LIBELLE_DISCIPLINE_AUTORISATION  TEXT,
        CODE_PROFESSION                  TEXT,
        LIBELLE_PROFESSION               TEXT
    );
    """
    cursor.execute(query_create_autexerc)

    query_create_activite = """
    CREATE TABLE IF NOT EXISTS ACTIVITE (
        TYPE_D_IDENTIFIANT_PP                   TEXT,
        IDENTIFIANT_PP                          TEXT,
        IDENTIFIANT_DE_L_ACTIVITE               TEXT,
        IDENTIFICATION_NATIONALE_PP             TEXT,
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE   TEXT,
        CODE_FONCTION                           TEXT,
        LIBELLE_FONCTION                        TEXT,
        CODE_MODE_EXERCICE                      TEXT,
        LIBELLE_MODE_EXERCICE                   TEXT,
        DATE_DE_DEBUT_ACTIVITE                  TEXT,
        DATE_DE_FIN_ACTIVITE                    TEXT,
        DATE_DE_MISE_A_JOUR_ACTIVITE            TEXT,
        CODE_REGION_EXERCICE                    TEXT,
        LIBELLE_REGION_EXERCICE                 TEXT,
        CODE_GENRE_ACTIVITE                     TEXT,
        LIBELLE_GENRE_ACTIVITE                  TEXT,
        CODE_MODIF_DE_FIN_D_ACTIVITE            TEXT,
        LIBELLE_MOTIF_DE_FIN_D_ACTIVITE         TEXT,
        CODE_SECTION_TABLEAU_PHARMACIENS        TEXT,
        LIBELLE_SECTION_TABLEAU_PHARMACIENS     TEXT,
        CODE_SOUSSECTION_TABLEAU_PHARMACIENS    TEXT,
        LIBELLE_SOUSSECTION_TABLEAU_PHARMACIENS TEXT,
        CODE_TYPE_ACTIVITE_LIBERALE             TEXT,
        LIBELLE_TYPE_ACTIVITE_LIBERALE          TEXT,
        CODE_STATUT_DES_PS_DU_SSA               TEXT,
        LIBELLE_STATUT_DES_PS_DU_SSA            TEXT,
        CODE_STATUT_HOSPITALIER                 TEXT,
        LIBELLE_STATUT_HOSPITALIER              TEXT,
        CODE_PROFESSION                         TEXT,
        LIBELLE_PROFESSION                      TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE          TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE       TEXT
    );
    """
    cursor.execute(query_create_activite)

    query_create_coordact = """
    CREATE TABLE IF NOT EXISTS COORDACT (
        TYPE_D_IDENTIFIANT_PP                        TEXT,
        IDENTIFIANT_PP                               TEXT,
        IDENTIFIANT_DE_L_ACTIVITE                    TEXT,
        IDENTIFICATION_NATIONALE_PP                  TEXT,
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE        TEXT,
        CODE_PROFESSION                              TEXT,
        LIBELLE_PROFESSION                           TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE               TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE            TEXT,
        COMPLEMENT_DESTINATAIRE_COORD_ACTIVITE       TEXT,
        COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_ACTIVITE TEXT,
        NUMERO_VOIE_COORD_ACTIVITE                   TEXT,
        INDICE_REPETITION_VOIE_COORD_ACTIVITE        TEXT,
        CODE_TYPE_DE_VOIE_COORD_ACTIVITE             TEXT,
        LIBELLE_TYPE_DE_VOIE_COORD_ACTIVITE          TEXT,
        LIBELLE_VOIE_COORD_ACTIVITE                  TEXT,
        MENTION_DISTRIBUTION_COORD_ACTIVITE          TEXT,
        BUREAU_CEDEX_COORD_ACTIVITE                  TEXT,
        CODE_POSTAL_COORD_ACTIVITE                   TEXT,
        CODE_COMMUNE_COORD_ACTIVITE                  TEXT,
        LIBELLE_COMMUNE_COORD_ACTIVITE               TEXT,
        CODE_PAYS_COORD_ACTIVITE                     TEXT,
        LIBELLE_PAYS_COORD_ACTIVITE                  TEXT,
        TELEPHONE_COORD_ACTIVITE                     TEXT,
        TELEPHONE_2_COORD_ACTIVITE                   TEXT,
        TELECOPIE_COORD_ACTIVITE                     TEXT,
        ADRESSE_EMAIL_COORD_ACTIVITE                 TEXT,
        DATE_DE_MISE_A_JOUR_COORD_ACTIVITE           TEXT,
        DATE_DE_FIN_COORD_ACTIVITE                   TEXT
    );
    """
    cursor.execute(query_create_coordact)

    query_create_coordcorresp = """
    CREATE TABLE IF NOT EXISTS COORDCORRESP (
        TYPE_D_IDENTIFIANT_PP                              TEXT,
        IDENTIFIANT_PP                                     TEXT,
        IDENTIFICATION_NATIONALE_PP                        TEXT,
        COMPLEMENT_DESTINATAIRE_COORD_CORRESPONDANCE       TEXT,
        COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_CORRESPONDANCE TEXT,
        NUMERO_VOIE_COORD_CORRESPONDANCE                   TEXT,
        INDICE_REPETITION_VOIE_COORD_CORRESPONDANCE        TEXT,
        CODE_TYPE_DE_VOIE_COORD_CORRESPONDANCE             TEXT,
        LIBELLE_TYPE_DE_VOIE_COORD_CORRESPONDANCE          TEXT,
        LIBELLE_VOIE_COORD_CORRESPONDANCE                  TEXT,
        MENTION_DISTRIBUTION_COORD_CORRESPONDANCE          TEXT,
        BUREAU_CEDEX_COORD_CORRESPONDANCE                  TEXT,
        CODE_POSTAL_COORD_CORRESPONDANCE                   TEXT,
        CODE_COMMUNE_COORD_CORRESPONDANCE                  TEXT,
        LIBELLE_COMMUNE_COORD_CORRESPONDANCE               TEXT,
        CODE_PAYS_COORD_CORRESPONDANCE                     TEXT,
        LIBELLE_PAYS_COORD_CORRESPONDANCE                  TEXT,
        TELEPHONE_COORD_CORRESPONDANCE                     TEXT,
        TELEPHONE_2_COORD_CORRESPONDANCE                   TEXT,
        TELECOPIE_COORD_CORRESPONDANCE                     TEXT,
        ADRESSE_EMAIL_COORD_CORRESPONDANCE                 TEXT,
        DATE_DE_MISE_A_JOUR_COORD_CORRESPONDANCE           TEXT,
        DATE_DE_FIN_COORD_CORRESPONDANCE                   TEXT
    );
    """
    cursor.execute(query_create_coordcorresp)

    query_create_coordstruct = """
    CREATE TABLE IF NOT EXISTS COORDSTRUCT (
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE         TEXT,
        COMPLEMENT_DESTINATAIRE_COORD_STRUCTURE       TEXT,
        COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_STRUCTURE TEXT,
        NUMERO_VOIE_COORD_STRUCTURE                   TEXT,
        INDICE_REPETITION_VOIE_COORD_STRUCTURE        TEXT,
        CODE_TYPE_DE_VOIE_COORD_STRUCTURE             TEXT,
        LIBELLE_TYPE_DE_VOIE_COORD_STRUCTURE          TEXT,
        LIBELLE_VOIE_COORD_STRUCTURE                  TEXT,
        MENTION_DISTRIBUTION_COORD_STRUCTURE          TEXT,
        BUREAU_CEDEX_COORD_STRUCTURE                  TEXT,
        CODE_POSTAL_COORD_STRUCTURE                   TEXT,
        CODE_COMMUNE_COORD_STRUCTURE                  TEXT,
        LIBELLE_COMMUNE_COORD_STRUCTURE               TEXT,
        CODE_PAYS_COORD_STRUCTURE                     TEXT,
        LIBELLE_PAYS_COORD_STRUCTURE                  TEXT,
        TELEPHONE_COORD_STRUCTURE                     TEXT,
        TELEPHONE_2_COORD_STRUCTURE                   TEXT,
        TELECOPIE_COORD_STRUCTURE                     TEXT,
        ADRESSE_EMAIL_COORD_STRUCTURE                 TEXT,
        DATE_DE_MISE_A_JOUR_COORD_STRUCTURE           TEXT,
        DATE_DE_FIN_COORD_STRUCTURE                   TEXT
    );
    """
    cursor.execute(query_create_coordstruct)

    query_create_diplobt = """
    CREATE TABLE IF NOT EXISTS DIPLOBT (
        TYPE_D_IDENTIFIANT_PP              TEXT,
        IDENTIFIANT_PP                     TEXT,
        IDENTIFICATION_NATIONALE_PP        TEXT,
        CODE_TYPE_DIPLOME_OBTENU           TEXT,
        LIBELLE_TYPE_DIPLOME_OBTENU        TEXT,
        CODE_DIPLOME_OBTENU                TEXT,
        LIBELLE_DIPLOME_OBTENU             TEXT,
        DATE_DE_MISE_A_JOUR_DIPLOME_OBTENU TEXT,
        CODE_LIEU_OBTENTION                TEXT,
        LIBELLE_LIEU_OBTENTION             TEXT,
        DATE_D_OBTENTION_DIPLOME           TEXT,
        NUMERO_DIPLOME                     TEXT
    );
    """
    cursor.execute(query_create_diplobt)

    query_create_etatciv = """
    CREATE TABLE IF NOT EXISTS ETATCIV (
        TYPE_D_IDENTIFIANT_PP         TEXT,
        IDENTIFIANT_PP                TEXT,
        IDENTIFICATION_NATIONALE_PP   TEXT,
        CODE_STATUT_ETATCIVIL         TEXT,
        LIBELLE_STATUT_ETATCIVIL      TEXT,
        CODE_SEXE                     TEXT,
        LIBELLE_SEXE                  TEXT,
        NOM_DE_FAMILLE                TEXT,
        PRENOMS                       TEXT,
        DATE_DE_NAISSANCE             TEXT,
        LIEU_DE_NAISSANCE             TEXT,
        DATE_DE_DECES                 TEXT,
        DATE_D_EFFET_DE_L_ETATCIVIL   TEXT,
        CODE_COMMUNE_DE_NAISSANCE     TEXT,
        LIBELLE_COMMUNE_DE_NAISSANCE  TEXT,
        CODE_PAYS_DE_NAISSANCE        TEXT,
        LIBELLE_PAYS_DE_NAISSANCE     TEXT,
        DATE_DE_MISE_A_JOUR_ETATCIVIL TEXT
    );
    """
    cursor.execute(query_create_etatciv)

    query_create_exercpro = """
    CREATE TABLE IF NOT EXISTS EXERCPRO (
        TYPE_D_IDENTIFIANT_PP              TEXT,
        IDENTIFIANT_PP                     TEXT,
        IDENTIFICATION_NATIONALE_PP        TEXT,
        CODE_CIVILITE_D_EXERCICE           TEXT,
        LIBELLE_CIVILITE_D_EXERCICE        TEXT,
        NOM_D_EXERCICE                     TEXT,
        PRENOM_D_EXERCICE                  TEXT,
        CODE_PROFESSION                    TEXT,
        LIBELLE_PROFESSION                 TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE     TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE  TEXT,
        DATE_DE_FIN_EXERCICE               TEXT,
        DATE_DE_MISE_A_JOUR_EXERCICE       TEXT,
        DATE_EFFET_EXERCICE                TEXT,
        CODE_AE_1E_INSCRIPTION             TEXT,
        LIBELLE_AE_1E_INSCRIPTION          TEXT,
        DATE_DEBUT_1E_INSCRIPTION          TEXT,
        DEPARTEMENT_1E_INSCRIPTION         TEXT,
        LIBELLE_DEPARTEMENT_1E_INSCRIPTION TEXT
    );
    """
    cursor.execute(query_create_exercpro)

    query_create_personne = """
    CREATE TABLE IF NOT EXISTS PERSONNE (
        TYPE_D_IDENTIFIANT_PP        TEXT,
        IDENTIFIANT_PP               TEXT,
        IDENTIFICATION_NATIONALE_PP  TEXT,
        CODE_CIVILITE                TEXT,
        LIBELLE_CIVILITE             TEXT,
        NOM_D_USAGE                  TEXT,
        PRENOM_D_USAGE               TEXT,
        NATURE                       TEXT,
        LIBELLE_NATIONALITE          TEXT,
        DATE_D_EFFET                 TEXT,
        DATE_DE_MISE_A_JOUR_PERSONNE TEXT
    );
    """
    cursor.execute(query_create_personne)

    query_create_referae = """
    CREATE TABLE IF NOT EXISTS REFERAE (
        TYPE_D_IDENTIFIANT_PP             TEXT,
        IDENTIFIANT_PP                    TEXT,
        IDENTIFICATION_NATIONALE_PP       TEXT,
        CODE_AE                           TEXT,
        LIBELLE_AE                        TEXT,
        DATE_DEBUT_INSCRIPTION            TEXT,
        DATE_FIN_INSCRIPTION              TEXT,
        DATE_DE_MISE_A_JOUR_INSCRIPTION   TEXT,
        CODE_SATUT_INSCRIPTION            TEXT,
        LIBELLE_STATUT_INSCRIPTION        TEXT,
        CODE_DEPARTEMENT_INSCRIPTION      TEXT,
        LIBELLE_DEPARTEMENT_INSCRIPTION   TEXT,
        CODE_DEPARTEMENT_ACCUEIL          TEXT,
        LIBELLE_DEPARTEMENT_ACCUEIL       TEXT,
        CODE_PROFESSION                   TEXT,
        LIBELLE_PROFESSION                TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE    TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE TEXT
    );
    """
    cursor.execute(query_create_referae)
 
    query_create_savoirfaire = """
    CREATE TABLE IF NOT EXISTS SAVOIRFAIRE (
        TYPE_D_IDENTIFIANT_PP             TEXT,
        IDENTIFIANT_PP                    TEXT,
        IDENTIFICATION_NATIONALE_PP       TEXT,
        CODE_SAVOIRFAIRE                  TEXT,
        LIBELLE_SAVOIRFAIRE               TEXT,
        CODE_TYPE_SAVOIRFAIRE             TEXT,
        LIBELLE_TYPE_SAVOIRFAIRE          TEXT,
        CODE_PROFESSION                   TEXT,
        LIBELLE_PROFESSION                TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE    TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE TEXT,
        DATE_RECONNAISSANCE_SAVOIRFAIRE   TEXT,
        DATE_DE_MISE_A_JOUR_SAVOIRFAIRE   TEXT,
        DATE_ABANDON_SAVOIRFAIRE          TEXT
    );
    """
    cursor.execute(query_create_savoirfaire)

    query_create_structure = """
    CREATE TABLE IF NOT EXISTS STRUCTURE (
        TYPE_DE_STRUCTURE                        TEXT,
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE    TEXT,
        IDENTIFICATION_NATIONALE_DE_LA_STRUCTURE TEXT,
        NUMERO_SIRET                             TEXT,
        NUMERO_SIREN                             TEXT,
        NUMERO_FINESS_ETABLISSEMENT              TEXT,
        NUMERO_FINESS_EJ                         TEXT,
        RPPS_RANG                                TEXT,
        ADELI_RANG                               TEXT,
        NUMERO_LICENCE_OFFICINE                  TEXT,
        DATE_D_OUVERTURE_STRUCTURE               TEXT,
        DATE_DE_FERMETURE_STRUCTURE              TEXT,
        DATE_DE_MISE_A_JOUR_STRUCTURE            TEXT,
        CODE_APE                                 TEXT,
        LIBELLE_APE                              TEXT,
        CODE_CATEGORIE_JURIDIQUE                 TEXT,
        LIBELLE_CATEGORIE_JURIDIQUE              TEXT,
        CODE_SECTEUR_D_ACTIVITE                  TEXT,
        LIBELLE_SECTEUR_D_ACTIVITE               TEXT,
        RAISON_SOCIALE                           TEXT,
        ENSEIGNE_COMMERCIALE                     TEXT
    );
    """
    cursor.execute(query_create_structure)

    query_create_insee_communes = """
    CREATE TABLE IF NOT EXISTS INSEE_COMMUNES (
        TYPECOM   TEXT,
        COM       TEXT,
        REG       TEXT,
        DEP       TEXT,
        ARR       TEXT,
        TNCC      TEXT,
        NCC       TEXT,
        NCCENR    TEXT,
        LIBELLE   TEXT,
        CAN       TEXT,
        COMPARENT TEXT
    );
    """
    cursor.execute(query_create_insee_communes)

    query_create_insee_departement = """
    CREATE TABLE IF NOT EXISTS INSEE_DEPARTEMENT (
        DEP      TEXT,
        REG      TEXT,
        CHEFLIEU TEXT,
        TNCC     TEXT,
        NCC      TEXT,
        NCCENR   TEXT,
        LIBELLE  TEXT
    );
    """
    cursor.execute(query_create_insee_departement)

    query_create_insee_region = """
    CREATE TABLE IF NOT EXISTS INSEE_REGION (
        REG      TEXT,
        CHEFLIEU TEXT,
        TNCC     TEXT,
        NCC      TEXT,
        NCCENR   TEXT,
        LIBELLE  TEXT
    );
    """
    cursor.execute(query_create_insee_region)

    cursor.close()
    conn.commit
    conn.close
    if verbose :
        print("Initialisation de la BDD terminée")
    return

# TEST 2
def insert_data_2(database = "database", verbose = True):
    if verbose :
        print(" --- Insertion des données --- ")
    conn = sqlite3.connect(database = database)
    cursor = conn.cursor()

    print(" --- Insertion des données depuis fichiers sources --- ")
    print(" ")
    #insert_data_from_source_files(conn)
   
    print(" --- Insertion des données depuis fichiers INSEE --- ")
    print(" ")
    insert_data_from_insee(conn)
    return

def insert_data_from_insee(conn, verbose = True):
    filenames_from_insee = get_filenames_from_insee()
    print(" --- Filenames_from_insee :", filenames_from_insee)

    for files in filenames_from_insee:
        print(" ------------------------------------------------------------------------ ")
        print(" --- Insertion des données depuis :", files)
        print(" ------------------------------------------------------------------------ ")

        filepath = "utils/" + files
        insert_file = pd.read_csv(filepath, sep=",")

        col = insert_file.columns
        print(" --- Nom des colonnes du fichier", files," :", col)
        print(" ")

        for elem in col:
            insert_file[[elem]] = insert_file[[elem]].astype(object)

        if files[:8] == "communes":
            table_name = "INSEE_COMMUNES"
        elif files[:11] == "departement":
            table_name = "INSEE_DEPARTEMENT"
        elif files[:6] == "region":
            table_name = "INSEE_REGION"

        print(" --- Nom de la table à complèter :", table_name)

        insert_file.to_sql(table_name, conn, if_exists = "replace", index = False)

        if verbose :
            print(" --- Insertion des données depuis le fichier", files, "vers la table", table_name, "réussie ---")
            print(" ")


def insert_data_from_source_files(conn, verbose = True):

    filenames_from_os = get_filenames_from_os()
    filenames_from_os.remove("Extraction_RPPS_Profil1_DiplObt.csv")

    print(" --- filenames_from_os : ", filenames_from_os)
    print(" ")

    for files in filenames_from_os:
        print(" ------------------------------------------------------------------------ ")
        print(" --- Insertion des données depuis : ", files)
        print(" ------------------------------------------------------------------------ ")
        filepath = "data/input/" + files
        insert_file = pd.read_csv(filepath, sep=";", low_memory = False)

        if insert_file.columns[-1][0:8]=="Unnamed:":
            col = insert_file.columns[:-1]
        else:
            col = insert_file.columns

        #col = insert_file.columns
        #if col[-1][0:8:]=="Unnamed:":
        #    col.remove(col[-1])

        print(" --- Nom des colonnes du fichier", files,": ", col)
        print(" ")

        for elem in col:
            insert_file[[elem]] = insert_file[[elem]].astype(object)

        table_name = files[24:-4:].upper()
        print(" --- Nom de la table à completer : ", table_name)

        insert_file.to_sql(table_name, conn, if_exists = "replace", index = False)

        if verbose :
            print(' --- Insertion des données depuis ', files, 'vers la table ', table_name, 'réussie --- ')
            print(" ")


def get_filenames_from_os():
    dict_filenames_from_os = os.listdir('data/input')
    files_from_os = []

    for elem in dict_filenames_from_os:
        if elem[-4::]=='.csv':
            files_from_os.append(elem)

    return files_from_os
 
def get_filenames_from_insee():
    dict_filenames_from_insee = os.listdir('utils/')
    files_from_insee = []

    for elem in dict_filenames_from_insee:
        if elem[-4::]=='.csv':
            files_from_insee.append(elem)

    return files_from_insee


#def insert_autexerc(database = "database", verbose = True):
 #   if verbose :
  #      print(" --- Insertion des données sources autexerc dans la BDD --- ")
   # conn = sqlite3.connect(database = database)
   # cursor = conn.cursor()
   # autexerc = pd.read_csv("data/input/Extraction_RPPS_Profil1_AutExerc.csv", sep = ";", dtype = {"Identifiant PP" : object})



# Création du schéma de dump


# Dump des données
## Données privées


## Données restreintes

