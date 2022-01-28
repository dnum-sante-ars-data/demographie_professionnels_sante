import os
import sqlite3
import json
import pandas as pd
from datetime import datetime, timedelta
from datetime import date
from icecream import ic
import pandas.io.sql as sqlio
import logging
import csv
import datetime

# Transformation activite
def transform_ods_activite(database = "database", verbose = True):
    print(" ----------------------------------------------------- ")
    print(" --- Remise en forme des données vers ODS_ACTIVITE --- ")
    print(" ----------------------------------------------------- ")
    print(" --- Connexion à la base de données")
    conn = sqlite3.connect(
        database = database
    )
    cursor = conn.cursor()

    print(" --- query_select_ods_activite")
    query_select_ods_activite = """
    SELECT
        dep.NCC as DEPARTEMENT_COORD_CORRESPONDANCE,
        reg.NCC as REGION_COORD_CORRESPONDANCE,
        a.TYPE_D_IDENTIFIANT_PP,
        a.IDENTIFIANT_PP,
        a.IDENTIFIANT_DE_L_ACTIVITE,
        a.IDENTIFICATION_NATIONALE_PP,
        a.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE,
        a.CODE_FONCTION,
        a.LIBELLE_FONCTION,
        a.CODE_MODE_EXERCICE,
        a.LIBELLE_MODE_EXERCICE,
        a.DATE_DE_DEBUT_ACTIVITE,
        a.DATE_DE_FIN_ACTIVITE,
        a.DATE_DE_MISE_A_JOUR_ACTIVITE,
        a.CODE_REGION_EXERCICE,
        a.LIBELLE_REGION_EXERCICE,
        a.CODE_GENRE_ACTIVITE,
        a.LIBELLE_GENRE_ACTIVITE,
        a.CODE_MOTIF_DE_FIN_D_ACTIVITE,
        a.LIBELLE_MOTIF_DE_FIN_D_ACTIVITE,
        a.CODE_SECTION_TABLEAU_PHARMACIENS,
        a.LIBELLE_SECTION_TABLEAU_PHARMACIENS,
        a.CODE_SOUS_SECTION_TABLEAU_PHARMACIENS,
        a.LIBELLE_SOUS_SECTION_TABLEAU_PHARMACIENS,
        a.CODE_TYPE_ACTIVITE_LIBERALE,
        a.LIBELLE_TYPE_ACTIVITE_LIBERALE,
        a.CODE_STATUT_DES_PS_DU_SSA,
        a.LIBELLE_STATUT_DES_PS_DU_SSA,
        a.CODE_STATUT_HOSPITALIER,
        a.LIBELLE_STATUT_HOSPITALIER,
        a.CODE_PROFESSION,
        a.LIBELLE_PROFESSION,
        a.CODE_CATEGORIE_PROFESSIONNELLE,
        a.LIBELLE_CATEGORIE_PROFESSIONNELLE,
        ca.COMPLEMENT_DESTINATAIRE_COORD_ACTIVITE,
        ca.COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_ACTIVITE,
        ca.NUMERO_VOIE_COORD_ACTIVITE,
        ca.INDICE_REPETITION_VOIE_COORD_ACTIVITE,
        ca.CODE_TYPE_DE_VOIE_COORD_ACTIVITE,
        ca.LIBELLE_TYPE_DE_VOIE_COORD_ACTIVITE,
        ca.LIBELLE_VOIE_COORD_ACTIVITE,
        ca.MENTION_DISTRIBUTION_COORD_ACTIVITE,
        ca.BUREAU_CEDEX_COORD_ACTIVITE,
        ca.CODE_POSTAL_COORD_ACTIVITE,
        ca.CODE_COMMUNE_COORD_ACTIVITE,
        ca.LIBELLE_COMMUNE_COORD_ACTIVITE,
        ca.CODE_PAYS_COORD_ACTIVITE,
        ca.LIBELLE_PAYS_COORD_ACTIVITE,
        ca.TELEPHONE_COORD_ACTIVITE,
        ca.TELEPHONE_2_COORD_ACTIVITE,
        ca.TELECOPIE_COORD_ACTIVITE,
        ca.ADRESSE_EMAIL_COORD_ACTIVITE,
        ca.DATE_DE_MISE_A_JOUR_COORD_ACTIVITE,
        ca.DATE_DE_FIN_COORD_ACTIVITE,
        s.NUMERO_SIRET,
        s.NUMERO_SIREN,
        s.NUMERO_FINESS_ETABLISSEMENT,
        s.NUMERO_FINESS_EJ,
        s.RPPS_RANG,
        s.ADELI_RANG,
        s.NUMERO_LICENCE_OFFICINE,
        s.DATE_D_OUVERTURE_STRUCTURE,
        s.DATE_DE_FERMETURE_STRUCTURE,
        s.DATE_DE_MISE_A_JOUR_STRUCTURE,
        s.CODE_APE,
        s.LIBELLE_APE,
        s.CODE_CATEGORIE_JURIDIQUE,
        s.LIBELLE_CATEGORIE_JURIDIQUE,
        s.CODE_SECTEUR_D_ACTIVITE,
        s.LIBELLE_SECTEUR_D_ACTIVITE,
        s.RAISON_SOCIALE,
        s.ENSEIGNE_COMMERCIALE,
        cs.NUMERO_VOIE_COORD_STRUCTURE,
        cs.INDICE_REPETITION_VOIE_COORD_STRUCTURE,
        cs.CODE_TYPE_DE_VOIE_COORD_STRUCTURE,
        cs.LIBELLE_TYPE_DE_VOIE_COORD_STRUCTURE,
        cs.LIBELLE_VOIE_COORD_STRUCTURE,
        cs.MENTION_DISTRIBUTION_COORD_STRUCTURE,
        cs.BUREAU_CEDEX_COORD_STRUCTURE,
        cs.CODE_POSTAL_COORD_STRUCTURE,
        cs.CODE_COMMUNE_COORD_STRUCTURE,
        cs.LIBELLE_COMMUNE_COORD_STRUCTURE,
        cs.CODE_PAYS_COORD_STRUCTURE,
        cs.LIBELLE_PAYS_COORD_STRUCTURE,
        cs.TELEPHONE_COORD_STRUCTURE,
        cs.TELEPHONE_2_COORD_STRUCTURE,
        cs.TELECOPIE_COORD_STRUCTURE,
        cs.ADRESSE_EMAIL_COORD_STRUCTURE,
        cs.DATE_DE_MISE_A_JOUR_COORD_STRUCTURE,
        cs.DATE_DE_FIN_COORD_STRUCTURE,
        ep.CODE_CIVILITE_D_EXERCICE,
        ep.LIBELLE_CIVILITE_D_EXERCICE,
        ep.NOM_D_EXERCICE,
        ep.PRENOM_D_EXERCICE,
        ep.DATE_DE_FIN_EXERCICE,
        ep.DATE_DE_MISE_A_JOUR_EXERCICE,
        ep.DATE_EFFET_EXERCICE,
        ep.CODE_AE_1E_INSCRIPTION,
        ep.LIBELLE_AE_1E_INSCRIPTION,
        ep.DATE_DEBUT_1E_INSCRIPTION,
        ep.DEPARTEMENT_1E_INSCRIPTION,
        ep.LIBELLE_DEPARTEMENT_1E_INSCRIPTION,
        sf.CODE_SAVOIR_FAIRE,
        sf.LIBELLE_SAVOIR_FAIRE,
        sf.CODE_TYPE_SAVOIR_FAIRE,
        sf.LIBELLE_TYPE_SAVOIR_FAIRE,
        sf.DATE_RECONNAISSANCE_SAVOIR_FAIRE,
        sf.DATE_DE_MISE_A_JOUR_SAVOIR_FAIRE,
        sf.DATE_ABANDON_SAVOIR_FAIRE,
        do.CODE_TYPE_DIPLOME_OBTENU,
        do.LIBELLE_TYPE_DIPLOME_OBTENU,
        do.CODE_DIPLOME_OBTENU,
        do.LIBELLE_DIPLOME_OBTENU,
        do.DATE_DE_MISE_A_JOUR_DIPLOME_OBTENU,
        do.CODE_LIEU_OBTENTION,
        do.LIBELLE_LIEU_OBTENTION,
        do.DATE_D_OBTENTION_DIPLOME,
        do.NUMERO_DIPLOME
    FROM ACTIVITE a
    LEFT JOIN COORDACT ca ON a.IDENTIFIANT_DE_L_ACTIVITE=ca.IDENTIFIANT_DE_L_ACTIVITE
    LEFT JOIN STRUCTURE s ON a.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE=s.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE
    LEFT JOIN COORDSTRUCT cs ON a.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE=cs.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE
    LEFT JOIN EXERCPRO ep ON a.IDENTIFIANT_PP=ep.IDENTIFIANT_PP
    LEFT JOIN SAVOIRFAIRE sf ON a.IDENTIFIANT_PP=sf.IDENTIFIANT_PP
    LEFT JOIN DIPLOBT do ON a.IDENTIFIANT_PP=do.IDENTIFIANT_PP
    LEFT JOIN INSEE_COMMUNES com ON ca.CODE_COMMUNE_COORD_ACTIVITE=com.com
    LEFT JOIN INSEE_DEPARTEMENT dep ON com.dep=dep.DEP
    LEFT JOIN INSEE_REGION reg ON dep.REG=reg.reg     
    """
    cursor.execute(query_select_ods_activite)
    selected_ods_activite_rows = []
    while True:
        selected_ods_activite_data = cursor.fetchone()
        if selected_ods_activite_data:
            selected_ods_activite_rows.append(tuple(selected_ods_activite_data))
        else:
            break

    print(" --- query_delete_ods_activite")
    query_delete_ods_activite = """
    DELETE FROM ODS_ACTIVITE
    """
    cursor.execute(query_delete_ods_activite)

    print(" --- query_insert_ods_activite")
    query_insert_ods_activite = """
    INSERT INTO ODS_ACTIVITE (
        DEPARTEMENT_COORD_CORRESPONDANCE,
        REGION_COORD_CORRESPONDANCE,
        TYPE_D_IDENTIFIANT_PP,
        IDENTIFIANT_PP,
        IDENTIFIANT_DE_L_ACTIVITE,
        IDENTIFICATION_NATIONALE_PP,
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE,
        CODE_FONCTION,
        LIBELLE_FONCTION,
        CODE_MODE_EXERCICE,
        LIBELLE_MODE_EXERCICE,
        DATE_DE_DEBUT_ACTIVITE,
        DATE_DE_FIN_ACTIVITE,
        DATE_DE_MISE_A_JOUR_ACTIVITE,
        CODE_REGION_EXERCICE,
        LIBELLE_REGION_EXERCICE,
        CODE_GENRE_ACTIVITE,
        LIBELLE_GENRE_ACTIVITE,
        CODE_MOTIF_DE_FIN_D_ACTIVITE,
        LIBELLE_MOTIF_DE_FIN_D_ACTIVITE,
        CODE_SECTION_TABLEAU_PHARMACIENS,
        LIBELLE_SECTION_TABLEAU_PHARMACIENS,
        CODE_SOUS_SECTION_TABLEAU_PHARMACIENS,
        LIBELLE_SOUS_SECTION_TABLEAU_PHARMACIENS,
        CODE_TYPE_ACTIVITE_LIBERALE,
        LIBELLE_TYPE_ACTIVITE_LIBERALE,
        CODE_STATUT_DES_PS_DU_SSA,
        LIBELLE_STATUT_DES_PS_DU_SSA,
        CODE_STATUT_HOSPITALIER,
        LIBELLE_STATUT_HOSPITALIER,
        CODE_PROFESSION,
        LIBELLE_PROFESSION,
        CODE_CATEGORIE_PROFESSIONNELLE,
        LIBELLE_CATEGORIE_PROFESSIONNELLE,
        COMPLEMENT_DESTINATAIRE_COORD_ACTIVITE,
        COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_ACTIVITE,
        NUMERO_VOIE_COORD_ACTIVITE,
        INDICE_REPETITION_VOIE_COORD_ACTIVITE,
        CODE_TYPE_DE_VOIE_COORD_ACTIVITE,
        LIBELLE_TYPE_DE_VOIE_COORD_ACTIVITE,
        LIBELLE_VOIE_COORD_ACTIVITE,
        MENTION_DISTRIBUTION_COORD_ACTIVITE,
        BUREAU_CEDEX_COORD_ACTIVITE,
        CODE_POSTAL_COORD_ACTIVITE,
        CODE_COMMUNE_COORD_ACTIVITE,
        LIBELLE_COMMUNE_COORD_ACTIVITE,
        CODE_PAYS_COORD_ACTIVITE,
        LIBELLE_PAYS_COORD_ACTIVITE,
        TELEPHONE_COORD_ACTIVITE,
        TELEPHONE_2_COORD_ACTIVITE,
        TELECOPIE_COORD_ACTIVITE,
        ADRESSE_EMAIL_COORD_ACTIVITE,
        DATE_DE_MISE_A_JOUR_COORD_ACTIVITE,
        DATE_DE_FIN_COORD_ACTIVITE,
        NUMERO_SIRET,
        NUMERO_SIREN,
        NUMERO_FINESS_ETABLISSEMENT,
        NUMERO_FINESS_EJ,
        RPPS_RANG,
        ADELI_RANG,
        NUMERO_LICENCE_OFFICINE,
        DATE_D_OUVERTURE_STRUCTURE,
        DATE_DE_FERMETURE_STRUCTURE,
        DATE_DE_MISE_A_JOUR_STRUCTURE,
        CODE_APE,
        LIBELLE_APE,
        CODE_CATEGORIE_JURIDIQUE,
        LIBELLE_CATEGORIE_JURIDIQUE,
        CODE_SECTEUR_D_ACTIVITE,
        LIBELLE_SECTEUR_D_ACTIVITE,
        RAISON_SOCIALE,
        ENSEIGNE_COMMERCIALE,
        NUMERO_VOIE_COORD_STRUCTURE,
        INDICE_REPETITION_VOIE_COORD_STRUCTURE,
        CODE_TYPE_DE_VOIE_COORD_STRUCTURE,
        LIBELLE_TYPE_DE_VOIE_COORD_STRUCTURE,
        LIBELLE_VOIE_COORD_STRUCTURE,
        MENTION_DISTRIBUTION_COORD_STRUCTURE,
        BUREAU_CEDEX_COORD_STRUCTURE,
        CODE_POSTAL_COORD_STRUCTURE,
        CODE_COMMUNE_COORD_STRUCTURE,
        LIBELLE_COMMUNE_COORD_STRUCTURE,
        CODE_PAYS_COORD_STRUCTURE,
        LIBELLE_PAYS_COORD_STRUCTURE,
        TELEPHONE_COORD_STRUCTURE,
        TELEPHONE_2_COORD_STRUCTURE,
        TELECOPIE_COORD_STRUCTURE,
        ADRESSE_EMAIL_COORD_STRUCTURE,
        DATE_DE_MISE_A_JOUR_COORD_STRUCTURE,
        DATE_DE_FIN_COORD_STRUCTURE,
        CODE_CIVILITE_D_EXERCICE,
        LIBELLE_CIVILITE_D_EXERCICE,
        NOM_D_EXERCICE,
        PRENOM_D_EXERCICE,
        DATE_DE_FIN_EXERCICE,
        DATE_DE_MISE_A_JOUR_EXERCICE,
        DATE_EFFET_EXERCICE,
        CODE_AE_1E_INSCRIPTION,
        LIBELLE_AE_1E_INSCRIPTION,
        DATE_DEBUT_1E_INSCRIPTION,
        DEPARTEMENT_1E_INSCRIPTION,
        LIBELLE_DEPARTEMENT_1E_INSCRIPTION,
        CODE_SAVOIR_FAIRE,
        LIBELLE_SAVOIR_FAIRE,
        CODE_TYPE_SAVOIR_FAIRE,
        LIBELLE_TYPE_SAVOIR_FAIRE,
        DATE_RECONNAISSANCE_SAVOIR_FAIRE,
        DATE_DE_MISE_A_JOUR_SAVOIR_FAIRE,
        DATE_ABANDON_SAVOIR_FAIRE,
        CODE_TYPE_DIPLOME_OBTENU,
        LIBELLE_TYPE_DIPLOME_OBTENU,
        CODE_DIPLOME_OBTENU,
        LIBELLE_DIPLOME_OBTENU,
        DATE_DE_MISE_A_JOUR_DIPLOME_OBTENU,
        CODE_LIEU_OBTENTION,
        LIBELLE_LIEU_OBTENTION,
        DATE_D_OBTENTION_DIPLOME,
        NUMERO_DIPLOME
    ) VALUES (
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    )
    """
    cursor.executemany(query_insert_ods_activite, selected_ods_activite_rows)
    conn.commit()
    cursor.close
    conn.close

# Transformation personne
def transform_ods_personne(database = "database", verbose = True):
    print(" ----------------------------------------------------- ")
    print(" --- Remise en forme des données vers ODS_PERSONNE --- ")
    print(" ----------------------------------------------------- ")
    print(" --- Connexion à la base de données")
    conn = sqlite3.connect(
        database = database
    )
    cursor = conn.cursor()
    
    print(" --- query_select_ods_personne")
    query_select_ods_personne = """
    SELECT
        dep.NCC as DEPARTEMENT_COORD_CORRESPONDANCE,
        reg.NCC as REGION_COORD_CORRESPONDANCE,
        p.TYPE_D_IDENTIFIANT_PP,
        p.IDENTIFIANT_PP,
        p.IDENTIFICATION_NATIONALE_PP,
        p.CODE_CIVILITE,
        p.LIBELLE_CIVILITE,
        p.NOM_D_USAGE,
        p.PRENOM_D_USAGE,
        p.NATURE,
        p.DATE_D_EFFET,
        p.DATE_DE_MISE_A_JOUR_PERSONNE,
        cc.COMPLEMENT_DESTINATAIRE_COORD_CORRESPONDANCE,
        cc.COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_CORRESPONDANCE,
        cc.NUMERO_VOIE_COORD_CORRESPONDANCE,
        cc.INDICE_REPETITION_VOIE_COORD_CORRESPONDANCE,
        cc.CODE_TYPE_DE_VOIE_COORD_CORRESPONDANCE,
        cc.LIBELLE_TYPE_DE_VOIE_COORD_CORRESPONDANCE,
        cc.LIBELLE_VOIE_COORD_CORRESPONDANCE,
        cc.MENTION_DISTRIBUTION_COORD_CORRESPONDANCE,
        cc.BUREAU_CEDEX_COORD_CORRESPONDANCE,
        cc.CODE_POSTAL_COORD_CORRESPONDANCE,
        cc.CODE_COMMUNE_COORD_CORRESPONDANCE,
        cc.LIBELLE_COMMUNE_COORD_CORRESPONDANCE,
        cc.CODE_PAYS_COORD_CORRESPONDANCE,
        cc.LIBELLE_PAYS_COORD_CORRESPONDANCE,
        cc.TELEPHONE_COORD_CORRESPONDANCE,
        cc.TELEPHONE_2_COORD_CORRESPONDANCE,
        cc.TELECOPIE_COORD_CORRESPONDANCE,
        cc.ADRESSE_EMAIL_COORD_CORRESPONDANCE,
        cc.DATE_DE_MISE_A_JOUR_COORD_CORRESPONDANCE,
        cc.DATE_DE_FIN_COORD_CORRESPONDANCE,
        ec.CODE_STATUT_ETAT_CIVIL,
        ec.LIBELLE_STATUT_ETAT_CIVIL,
        ec.CODE_SEXE,
        ec.LIBELLE_SEXE,
        ec.NOM_DE_FAMILLE,
        ec.PRENOMS,
        ec.DATE_DE_NAISSANCE,
        ec.LIEU_DE_NAISSANCE,
        ec.DATE_DE_DECES,
        ec.DATE_D_EFFET_DE_L_ETAT_CIVIL,
        ec.CODE_COMMUNE_DE_NAISSANCE,
        ec.LIBELLE_COMMUNE_DE_NAISSANCE,
        ec.CODE_PAYS_DE_NAISSANCE,
        ec.LIBELLE_PAYS_DE_NAISSANCE,
        ec.DATE_DE_MISE_A_JOUR_ETAT_CIVIL,
        ae.DATE_EFFET_AUTORISATION,
        ae.CODE_TYPE_AUTORISATION,
        ae.LIBELLE_TYPE_AUTORISATION,
        ae.DATE_FIN_AUTORISATION,
        ae.DATE_DE_MISE_A_JOUR_AUTORISATION,
        ae.CODE_DISCIPLINE_AUTORISATION,
        ae.LIBELLE_DISCIPLINE_AUTORISATION,
        ra.CODE_AE,
        ra.LIBELLE_AE,
        ra.DATE_DEBUT_INSCRIPTION,
        ra.DATE_FIN_INSCRIPTION,
        ra.DATE_DE_MISE_A_JOUR_INSCRIPTION,
        ra.CODE_STATUT_INSCRIPTION,
        ra.LIBELLE_STATUT_INSCRIPTION,
        ra.CODE_DEPARTEMENT_INSCRIPTION,
        ra.LIBELLE_DEPARTEMENT_INSCRIPTION,
        ra.CODE_DEPARTEMENT_ACCUEIL,
        ra.LIBELLE_DEPARTEMENT_ACCUEIL
    FROM PERSONNE p
    LEFT JOIN COORDCORRESP cc ON p.IDENTIFIANT_PP=cc.IDENTIFIANT_PP
    LEFT JOIN ETATCIV ec ON p.IDENTIFIANT_PP=ec.IDENTIFIANT_PP
    LEFT JOIN AUTEXERC ae ON p.IDENTIFIANT_PP=ae.IDENTIFIANT_PP
    LEFT JOIN REFERAE ra ON p.IDENTIFIANT_PP=ra.IDENTIFIANT_PP
    LEFT JOIN INSEE_COMMUNES com ON cc.CODE_COMMUNE_COORD_CORRESPONDANCE=com.com
    LEFT JOIN INSEE_DEPARTEMENT dep ON com.dep=dep.DEP
    LEFT JOIN INSEE_REGION reg ON dep.REG=reg.reg 
    """
    cursor.execute(query_select_ods_personne)
    selected_ods_personne_rows = []
    while True:
        selected_ods_personne_data = cursor.fetchone()
        if selected_ods_personne_data:
            selected_ods_personne_rows.append(tuple(selected_ods_personne_data))
        else:
            break
    
    print(" --- query_delete_ods_personne")
    query_delete_ods_personne = """
    DELETE FROM ODS_PERSONNE
    """
    cursor.execute(query_delete_ods_personne)

    print(" --- query_insert_ods_personne")
    query_insert_ods_personne = """
    INSERT INTO ODS_PERSONNE (
        DEPARTEMENT_COORD_CORRESPONDANCE,
        REGION_COORD_CORRESPONDANCE,
        TYPE_D_IDENTIFIANT_PP,
        IDENTIFIANT_PP,
        IDENTIFICATION_NATIONALE_PP,
        CODE_CIVILITE,
        LIBELLE_CIVILITE,
        NOM_D_USAGE,
        PRENOM_D_USAGE,
        NATURE,
        DATE_D_EFFET,
        DATE_DE_MISE_A_JOUR_PERSONNE,
        COMPLEMENT_DESTINATAIRE_COORD_CORRESPONDANCE,
        COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_CORRESPONDANCE,
        NUMERO_VOIE_COORD_CORRESPONDANCE,
        INDICE_REPETITION_VOIE_COORD_CORRESPONDANCE,
        CODE_TYPE_DE_VOIE_COORD_CORRESPONDANCE,
        LIBELLE_TYPE_DE_VOIE_COORD_CORRESPONDANCE,
        LIBELLE_VOIE_COORD_CORRESPONDANCE,
        MENTION_DISTRIBUTION_COORD_CORRESPONDANCE,
        BUREAU_CEDEX_COORD_CORRESPONDANCE,
        CODE_POSTAL_COORD_CORRESPONDANCE,
        CODE_COMMUNE_COORD_CORRESPONDANCE,
        LIBELLE_COMMUNE_COORD_CORRESPONDANCE,
        CODE_PAYS_COORD_CORRESPONDANCE,
        LIBELLE_PAYS_COORD_CORRESPONDANCE,
        TELEPHONE_COORD_CORRESPONDANCE,
        TELEPHONE_2_COORD_CORRESPONDANCE,
        TELECOPIE_COORD_CORRESPONDANCE,
        ADRESSE_EMAIL_COORD_CORRESPONDANCE,
        DATE_DE_MISE_A_JOUR_COORD_CORRESPONDANCE,
        DATE_DE_FIN_COORD_CORRESPONDANCE,
        CODE_STATUT_ETAT_CIVIL,
        LIBELLE_STATUT_ETAT_CIVIL,
        CODE_SEXE,
        LIBELLE_SEXE,
        NOM_DE_FAMILLE,
        PRENOMS,
        DATE_DE_NAISSANCE,
        LIEU_DE_NAISSANCE,
        DATE_DE_DECES,
        DATE_D_EFFET_DE_L_ETAT_CIVIL,
        CODE_COMMUNE_DE_NAISSANCE,
        LIBELLE_COMMUNE_DE_NAISSANCE,
        CODE_PAYS_DE_NAISSANCE,
        LIBELLE_PAYS_DE_NAISSANCE,
        DATE_DE_MISE_A_JOUR_ETAT_CIVIL,
        DATE_EFFET_AUTORISATION,
        CODE_TYPE_AUTORISATION,
        LIBELLE_TYPE_AUTORISATION,
        DATE_FIN_AUTORISATION,
        DATE_DE_MISE_A_JOUR_AUTORISATION,
        CODE_DISCIPLINE_AUTORISATION,
        LIBELLE_DISCIPLINE_AUTORISATION,
        CODE_AE,
        LIBELLE_AE,
        DATE_DEBUT_INSCRIPTION,
        DATE_FIN_INSCRIPTION,
        DATE_DE_MISE_A_JOUR_INSCRIPTION,
        CODE_STATUT_INSCRIPTION,
        LIBELLE_STATUT_INSCRIPTION,
        CODE_DEPARTEMENT_INSCRIPTION,
        LIBELLE_DEPARTEMENT_INSCRIPTION,
        CODE_DEPARTEMENT_ACCUEIL,
        LIBELLE_DEPARTEMENT_ACCUEIL
    ) VALUES (
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    )
    """
    cursor.executemany(query_insert_ods_personne, selected_ods_personne_rows)
    conn.commit()
    cursor.close
    conn.close
    


# Test export csv
def transform_to_csv(database = "database", verbose = True):
    conn = sqlite3.connect(
        database = database
    )
    
    print(" --- Extraction vers ODS_PERSONNE.csv en cours")
    df = pd.read_sql('SELECT * FROM ODS_PERSONNE', conn)
    df.to_csv('data/output/ODS_PERSONNE.csv', index = False)
    print(" --- Extraction vers ODS_PERSONNE.csv réalisée")
    print(" ")
    print(" --- Extraction vers ODS_ACTIVITE.csv en cours")
    df2 = pd.read_sql('SELECT * FROM ODS_ACTIVITE', conn)
    df2.to.csv('data/output/ODS_ACTIVITE.csv', index = False)
    print(" --- Extraction vers ODS_ACTIVITE.csv réalisée")


def transform_export(filepath_activites, filepath_personnes, database="database", verbose = True) :
    """
    Transformations et export des activités
    """
    conn = sqlite3.connect(
        database=database
    )
    print(" - - - Suppression des fichiers output si existants...")
    if os.path.exists(filepath_activites):
        os.remove(filepath_activites)
    if os.path.exists(filepath_personnes):
        os.remove(filepath_personnes)
    print(" - - - Transformation et export des données activités...")
    select_activites_sql = """
        SELECT
            dep.NCC as DEPARTEMENT_COORD_CORRESPONDANCE,
            reg.NCC as REGION_COORD_CORRESPONDANCE,
            a.TYPE_D_IDENTIFIANT_PP,
            a.IDENTIFIANT_PP,
            a.IDENTIFIANT_DE_L_ACTIVITE,
            a.IDENTIFICATION_NATIONALE_PP,
            a.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE,
            a.CODE_FONCTION,
            a.LIBELLE_FONCTION,
            a.CODE_MODE_EXERCICE,
            a.LIBELLE_MODE_EXERCICE,
            a.DATE_DE_DEBUT_ACTIVITE,
            a.DATE_DE_FIN_ACTIVITE,
            a.DATE_DE_MISE_A_JOUR_ACTIVITE,
            a.CODE_REGION_EXERCICE,
            a.LIBELLE_REGION_EXERCICE,
            a.CODE_GENRE_ACTIVITE,
            a.LIBELLE_GENRE_ACTIVITE,
            a.CODE_MOTIF_DE_FIN_D_ACTIVITE,
            a.LIBELLE_MOTIF_DE_FIN_D_ACTIVITE,
            a.CODE_SECTION_TABLEAU_PHARMACIENS,
            a.LIBELLE_SECTION_TABLEAU_PHARMACIENS,
            a.CODE_SOUS_SECTION_TABLEAU_PHARMACIENS,
            a.LIBELLE_SOUS_SECTION_TABLEAU_PHARMACIENS,
            a.CODE_TYPE_ACTIVITE_LIBERALE,
            a.LIBELLE_TYPE_ACTIVITE_LIBERALE,
            a.CODE_STATUT_DES_PS_DU_SSA,
            a.LIBELLE_STATUT_DES_PS_DU_SSA,
            a.CODE_STATUT_HOSPITALIER,
            a.LIBELLE_STATUT_HOSPITALIER,
            a.CODE_PROFESSION,
            a.LIBELLE_PROFESSION,
            a.CODE_CATEGORIE_PROFESSIONNELLE,
            a.LIBELLE_CATEGORIE_PROFESSIONNELLE,
            ca.COMPLEMENT_DESTINATAIRE_COORD_ACTIVITE,
            ca.COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_ACTIVITE,
            ca.NUMERO_VOIE_COORD_ACTIVITE,
            ca.INDICE_REPETITION_VOIE_COORD_ACTIVITE,
            ca.CODE_TYPE_DE_VOIE_COORD_ACTIVITE,
            ca.LIBELLE_TYPE_DE_VOIE_COORD_ACTIVITE,
            ca.LIBELLE_VOIE_COORD_ACTIVITE,
            ca.MENTION_DISTRIBUTION_COORD_ACTIVITE,
            ca.BUREAU_CEDEX_COORD_ACTIVITE,
            ca.CODE_POSTAL_COORD_ACTIVITE,
            ca.CODE_COMMUNE_COORD_ACTIVITE,
            ca.LIBELLE_COMMUNE_COORD_ACTIVITE,
            ca.CODE_PAYS_COORD_ACTIVITE,
            ca.LIBELLE_PAYS_COORD_ACTIVITE,
            ca.TELEPHONE_COORD_ACTIVITE,
            ca.TELEPHONE_2_COORD_ACTIVITE,
            ca.TELECOPIE_COORD_ACTIVITE,
            ca.ADRESSE_EMAIL_COORD_ACTIVITE,
            ca.DATE_DE_MISE_A_JOUR_COORD_ACTIVITE,
            ca.DATE_DE_FIN_COORD_ACTIVITE,
            s.NUMERO_SIRET,
            s.NUMERO_SIREN,
            s.NUMERO_FINESS_ETABLISSEMENT,
            s.NUMERO_FINESS_EJ,
            s.RPPS_RANG,
            s.ADELI_RANG,
            s.NUMERO_LICENCE_OFFICINE,
            s.DATE_D_OUVERTURE_STRUCTURE,
            s.DATE_DE_FERMETURE_STRUCTURE,
            s.DATE_DE_MISE_A_JOUR_STRUCTURE,
            s.CODE_APE,
            s.LIBELLE_APE,
            s.CODE_CATEGORIE_JURIDIQUE,
            s.LIBELLE_CATEGORIE_JURIDIQUE,
            s.CODE_SECTEUR_D_ACTIVITE,
            s.LIBELLE_SECTEUR_D_ACTIVITE,
            s.RAISON_SOCIALE,
            s.ENSEIGNE_COMMERCIALE,
            cs.NUMERO_VOIE_COORD_STRUCTURE,
            cs.INDICE_REPETITION_VOIE_COORD_STRUCTURE,
            cs.CODE_TYPE_DE_VOIE_COORD_STRUCTURE,
            cs.LIBELLE_TYPE_DE_VOIE_COORD_STRUCTURE,
            cs.LIBELLE_VOIE_COORD_STRUCTURE,
            cs.MENTION_DISTRIBUTION_COORD_STRUCTURE,
            cs.BUREAU_CEDEX_COORD_STRUCTURE,
            cs.CODE_POSTAL_COORD_STRUCTURE,
            cs.CODE_COMMUNE_COORD_STRUCTURE,
            cs.LIBELLE_COMMUNE_COORD_STRUCTURE,
            cs.CODE_PAYS_COORD_STRUCTURE,
            cs.LIBELLE_PAYS_COORD_STRUCTURE,
            cs.TELEPHONE_COORD_STRUCTURE,
            cs.TELEPHONE_2_COORD_STRUCTURE,
            cs.TELECOPIE_COORD_STRUCTURE,
            cs.ADRESSE_EMAIL_COORD_STRUCTURE,
            cs.DATE_DE_MISE_A_JOUR_COORD_STRUCTURE,
            cs.DATE_DE_FIN_COORD_STRUCTURE
        FROM ACTIVITE a
        LEFT JOIN COORDACT ca ON a.IDENTIFIANT_DE_L_ACTIVITE=ca.IDENTIFIANT_DE_L_ACTIVITE
        LEFT JOIN STRUCTURE s ON a.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE=s.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE
        LEFT JOIN COORDSTRUCT cs ON a.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE=cs.IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE
        LEFT JOIN INSEE_COMMUNES com ON ca.CODE_COMMUNE_COORD_ACTIVITE=com.com
        LEFT JOIN INSEE_DEPARTEMENT dep ON com.dep=dep.DEP
        LEFT JOIN INSEE_REGION reg ON dep.REG=reg.reg;
        """
    for chunk in pd.read_sql_query(select_activites_sql, conn, chunksize=10000):
       chunk.to_csv(os.path.join(filepath_activites), mode='a',sep=';',encoding='utf-8')
    print(" - - - Transformations et export des données personnes...")
    select_personnes_sql = """
        SELECT
            dep.NCC as DEPARTEMENT_COORD_CORRESPONDANCE,
            reg.NCC as REGION_COORD_CORRESPONDANCE,
            p.TYPE_D_IDENTIFIANT_PP,
            p.IDENTIFIANT_PP,
            p.IDENTIFICATION_NATIONALE_PP,
            p.CODE_CIVILITE,
            p.LIBELLE_CIVILITE,
            p.NOM_D_USAGE,
            p.PRENOM_D_USAGE,
            p.NATURE,
            p.DATE_D_EFFET,
            p.DATE_DE_MISE_A_JOUR_PERSONNE,
            cc.COMPLEMENT_DESTINATAIRE_COORD_CORRESPONDANCE,
            cc.COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_CORRESPONDANCE,
            cc.NUMERO_VOIE_COORD_CORRESPONDANCE,
            cc.INDICE_REPETITION_VOIE_COORD_CORRESPONDANCE,
            cc.CODE_TYPE_DE_VOIE_COORD_CORRESPONDANCE,
            cc.LIBELLE_TYPE_DE_VOIE_COORD_CORRESPONDANCE,
            cc.LIBELLE_VOIE_COORD_CORRESPONDANCE,
            cc.MENTION_DISTRIBUTION_COORD_CORRESPONDANCE,
            cc.BUREAU_CEDEX_COORD_CORRESPONDANCE,
            cc.CODE_POSTAL_COORD_CORRESPONDANCE,
            cc.CODE_COMMUNE_COORD_CORRESPONDANCE,
            cc.LIBELLE_COMMUNE_COORD_CORRESPONDANCE,
            cc.CODE_PAYS_COORD_CORRESPONDANCE,
            cc.LIBELLE_PAYS_COORD_CORRESPONDANCE,
            cc.TELEPHONE_COORD_CORRESPONDANCE,
            cc.TELEPHONE_2_COORD_CORRESPONDANCE,
            cc.TELECOPIE_COORD_CORRESPONDANCE,
            cc.ADRESSE_EMAIL_COORD_CORRESPONDANCE,
            cc.DATE_DE_MISE_A_JOUR_COORD_CORRESPONDANCE,
            cc.DATE_DE_FIN_COORD_CORRESPONDANCE,
            ec.CODE_STATUT_ETAT_CIVIL,
            ec.LIBELLE_STATUT_ETAT_CIVIL,
            ec.CODE_SEXE,
            ec.LIBELLE_SEXE,
            ec.NOM_DE_FAMILLE,
            ec.PRENOMS,
            ec.DATE_DE_NAISSANCE,
            ec.LIEU_DE_NAISSANCE,
            ec.DATE_DE_DECES,
            ec.DATE_D_EFFET_DE_L_ETAT_CIVIL,
            ec.CODE_COMMUNE_DE_NAISSANCE,
            ec.LIBELLE_COMMUNE_DE_NAISSANCE,
            ec.CODE_PAYS_DE_NAISSANCE,
            ec.LIBELLE_PAYS_DE_NAISSANCE,
            ec.DATE_DE_MISE_A_JOUR_ETAT_CIVIL,
            ae.DATE_EFFET_AUTORISATION,
            ae.CODE_TYPE_AUTORISATION,
            ae.LIBELLE_TYPE_AUTORISATION,
            ae.DATE_FIN_AUTORISATION,
            ae.DATE_DE_MISE_A_JOUR_AUTORISATION,
            ae.CODE_DISCIPLINE_AUTORISATION,
            ae.LIBELLE_DISCIPLINE_AUTORISATION,
            ra.CODE_AE,
            ra.LIBELLE_AE,
            ra.DATE_DEBUT_INSCRIPTION,
            ra.DATE_FIN_INSCRIPTION,
            ra.DATE_DE_MISE_A_JOUR_INSCRIPTION,
            ra.CODE_STATUT_INSCRIPTION,
            ra.LIBELLE_STATUT_INSCRIPTION,
            ra.CODE_DEPARTEMENT_INSCRIPTION,
            ra.LIBELLE_DEPARTEMENT_INSCRIPTION,
            ra.CODE_DEPARTEMENT_ACCUEIL,
            ra.LIBELLE_DEPARTEMENT_ACCUEIL,
            ep.CODE_CIVILITE_D_EXERCICE,
            ep.LIBELLE_CIVILITE_D_EXERCICE,
            ep.NOM_D_EXERCICE,
            ep.PRENOM_D_EXERCICE,
            ep.DATE_DE_FIN_EXERCICE,
            ep.DATE_DE_MISE_A_JOUR_EXERCICE,
            ep.DATE_EFFET_EXERCICE,
            ep.CODE_AE_1E_INSCRIPTION,
            ep.LIBELLE_AE_1E_INSCRIPTION,
            ep.DATE_DEBUT_1E_INSCRIPTION,
            ep.DEPARTEMENT_1E_INSCRIPTION,
            ep.LIBELLE_DEPARTEMENT_1E_INSCRIPTION,
            sf.CODE_SAVOIR_FAIRE,
            sf.LIBELLE_SAVOIR_FAIRE,
            sf.CODE_TYPE_SAVOIR_FAIRE,
            sf.LIBELLE_TYPE_SAVOIR_FAIRE,
            sf.DATE_RECONNAISSANCE_SAVOIR_FAIRE,
            sf.DATE_DE_MISE_A_JOUR_SAVOIR_FAIRE,
            sf.DATE_ABANDON_SAVOIR_FAIRE,
            do.CODE_TYPE_DIPLOME_OBTENU,
            do.LIBELLE_TYPE_DIPLOME_OBTENU,
            do.CODE_DIPLOME_OBTENU,
            do.LIBELLE_DIPLOME_OBTENU,
            do.DATE_DE_MISE_A_JOUR_DIPLOME_OBTENU,
            do.CODE_LIEU_OBTENTION,
            do.LIBELLE_LIEU_OBTENTION,
            do.DATE_D_OBTENTION_DIPLOME,
            do.NUMERO_DIPLOME
        FROM PERSONNE p
        LEFT JOIN COORDCORRESP cc ON p.IDENTIFIANT_PP=cc.IDENTIFIANT_PP
        LEFT JOIN ETATCIV ec ON p.IDENTIFIANT_PP=ec.IDENTIFIANT_PP
        LEFT JOIN AUTEXERC ae ON p.IDENTIFIANT_PP=ae.IDENTIFIANT_PP
        LEFT JOIN REFERAE ra ON p.IDENTIFIANT_PP=ra.IDENTIFIANT_PP
        LEFT JOIN EXERCPRO ep ON p.IDENTIFIANT_PP=ep.IDENTIFIANT_PP
        LEFT JOIN SAVOIRFAIRE sf ON p.IDENTIFIANT_PP=sf.IDENTIFIANT_PP
        LEFT JOIN DIPLOBT do ON p.IDENTIFIANT_PP=do.IDENTIFIANT_PP
        LEFT JOIN INSEE_COMMUNES com ON cc.CODE_COMMUNE_COORD_CORRESPONDANCE=com.com
        LEFT JOIN INSEE_DEPARTEMENT dep ON com.dep=dep.DEP
        LEFT JOIN INSEE_REGION reg ON dep.REG=reg.reg ;
        """
    for chunk in pd.read_sql_query(select_personnes_sql, conn, chunksize=10000):
       chunk.to_csv(os.path.join(filepath_personnes), mode='a',sep=';',encoding='utf-8')
