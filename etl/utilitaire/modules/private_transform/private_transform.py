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
        s.ENSEIGNE_COMMERCIAlE,
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
    LEFT JOIN COORDACT ca ON a.IDENTIFIANT_PP=ca.IDENTIFIANT_PP
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
    


# transformation sur les données démographiques des professionnels de santé
def transform_f_libreacces_ps (database="database", verbose = True) :
    print(" - - - Remise en forme des données démographiques des professionels de santé ...")
    conn = sqlite3.connect(
        database=database
    )
    cursor = conn.cursor()
    query_select_sa_libreacces = """
    SELECT 
        splpa."Type d'identifiant PP" AS type_d_identifiant_pp,
        splpa."Identifiant PP" AS identifiant_pp,
        splpa."Identification nationale PP" AS identification_nationale_pp,
        splpa."Code civilité d'exercice" AS code_civilite_d_exercice,
        splpa."Libellé civilité d'exercice" AS libelle_civilite_d_exercice,
        splpa."Code civilité" AS code_civilite,
        splpa."Libellé civilité" AS libelle_civilite,
        splpa."Nom d'exercice" AS nom_d_exercice,
        splpa."Prénom d'exercice" AS prenom_d_exercice,
        splpa."Code profession" AS code_profession,
        splpa."Libellé profession" AS libelle_profession,
        splpa."Code catégorie professionnelle" AS code_categorie_professionnelle,
        splpa."Libellé catégorie professionnelle" AS libelle_categorie_professionnelle,
        splpa."Code type savoir-faire" AS code_type_savoir_faire,
        splpa."Libellé type savoir-faire" AS libelle_type_savoir_faire,
        splpa."Code savoir-faire" AS code_savoir_faire,
        splpa."Libellé savoir-faire" AS libelle_savoir_faire,
        splpa."Code mode exercice" AS code_mode_exercice,
        splpa."Libellé mode exercice" AS libelle_mode_exercice,
        splpa."Numéro SIRET site" AS numero_siret_site,
        splpa."Numéro SIREN site" AS numero_siren_site,
        splpa."Numéro FINESS site" AS numero_finess_site,
        splpa."Numéro FINESS établissement juridique" AS numero_finess_etablissement_juridique,
        splpa."Identifiant technique de la structure" AS identifiant_technique_de_la_structure,
        splpa."Raison sociale site" AS raison_sociale_site,
        splpa."Enseigne commerciale site" AS enseigne_commerciale_site,
        splpa."Complément destinataire (coord. structure)" AS complement_destinataire_coord_structure,
        splpa."Complément point géographique (coord. structure)" AS complement_point_geographique_coord_structure,
        splpa."Numéro Voie (coord. structure)" AS numero_voie_coord_structure,
        splpa."Indice répétition voie (coord. structure)" AS indice_repetition_voie_coord_structure,
        splpa."Code type de voie (coord. structure)" AS code_type_de_voie_coord_structure,
        splpa."Libellé type de voie (coord. structure)" AS libelle_type_de_voie_coord_structure,
        splpa."Libellé Voie (coord. structure)" AS libelle_voie_coord_structure,
        splpa."Mention distribution (coord. structure)" AS mention_distribution_coord_structure,
        splpa."Bureau cedex (coord. structure)" AS bureau_cedex_coord_structure,
        splpa."Code postal (coord. structure)" AS code_postal_coord_structure,
        splpa."Code commune (coord. structure)" AS code_commune_coord_structure,
        splpa."Libellé commune (coord. structure)" AS libelle_commune_coord_structure,
        splpa."Code pays (coord. structure)" AS code_pays_coord_structure,
        splpa."Libellé pays (coord. structure)" AS libelle_pays_coord_structure,
        splpa."Téléphone (coord. structure)" AS telephone_coord_structure,
        splpa."Téléphone 2 (coord. structure)" AS telephone_2_coord_structure,
        splpa."Télécopie (coord. structure)" AS telecopie_coord_structure,
        splpa."Adresse e-mail (coord. structure)" AS adresse_e_mail_coord_structure,
        splpa."Code Département (structure)" AS code_departement_structure,
        splpa."Libellé Département (structure)" AS libelle_departement_structure,
        splpa."Ancien identifiant de la structure" AS ancien_identifiant_de_la_structure,
        splpa."Autorité d'enregistrement" AS autorite_d_enregistrement,
        splpa."Code secteur d'activité" AS code_secteur_d_activite,
        splpa."Libellé secteur d'activité" AS libelle_secteur_d_activite,
        splpa."Code section tableau pharmaciens" AS code_section_tableau_pharmaciens,
        splpa."Libellé section tableau pharmaciens" AS libelle_section_tableau_pharmaciens,
        splpa."Unnamed: 52" AS unnamed_52,
        splda."Code type diplôme obtenu" AS code_type_diplome_obtenu,
        splda."Libellé type diplôme obtenu" AS libelle_type_diplome_obtenu,
        splda."Code diplôme obtenu" AS code_diplome_obtenu,
        splda."Libellé diplôme obtenu" AS libelle_diplome_obtenu,
        splda."Code type autorisation" AS code_type_autorisation,
        splda."Libellé type autorisation" AS libelle_type_autorisation,
        splda."Code discipline autorisation" AS code_discipline_autorisation,
        splda."Libellé discipline autorisation" AS libelle_discipline_autorisation,
        splda."Unnamed: 13" AS splda_unnamed_13,
        spls."Code profession" AS spls_code_profession,
        spls."Libellé profession" AS spls_libelle_profession,
        spls."Code catégorie professionnelle" AS spls_code_categorie_professionnelle,
        spls."Libellé catégorie professionnelle" AS spls_libelle_categorie_professionnelle,
        spls."Code type savoir-faire" AS spls_code_type_savoir_faire,
        spls."Libellé type savoir-faire" AS spls_libelle_type_savoir_faire,
        spls."Code savoir-faire" AS spls_code_savoir_faire,
        spls."Libellé savoir-faire" AS spls_libelle_savoir_faire,
        spls."Unnamed: 13" AS spls_unnamed_13
    FROM sa_ps_libreacces_personne_activite splpa 
    LEFT JOIN sa_ps_libreacces_dipl_autexerc splda ON splpa."Identifiant PP"=splda."Identifiant PP" 
    LEFT JOIN sa_ps_libreacces_savoirfaire spls ON splpa."Identifiant PP"=spls."Identifiant PP" 
    """
    cursor.execute(query_select_sa_libreacces)
    selected_sa_libreacces_rows = []
    while True :
        selected_sa_libreacces_data = cursor.fetchone()
        if selected_sa_libreacces_data :
            selected_sa_libreacces_rows.append(tuple(selected_sa_libreacces_data))
        else :
            break
    query_delete_F_PS = """
    DELETE FROM F_PS
    """
    cursor.execute(query_delete_F_PS)
    query_insert_F_PS = """
    INSERT INTO F_PS (
        TYPE_D_IDENTIFIANT_PP,
        IDENTIFIANT_PP,
        IDENTIFICATION_NATIONALE_PP,
        CODE_CIVILITE_D_EXERCICE,
        LIBELLE_CIVILITE_D_EXERCICE,
        CODE_CIVILITE,
        LIBELLE_CIVILITE,
        NOM_D_EXERCICE,
        PRENOM_D_EXERCICE,
        CODE_PROFESSION,
        LIBELLE_PROFESSION,
        CODE_CATEGORIE_PROFESSIONNELLE,
        LIBELLE_CATEGORIE_PROFESSIONNELLE,
        CODE_TYPE_SAVOIR_FAIRE,
        LIBELLE_TYPE_SAVOIR_FAIRE,
        CODE_SAVOIR_FAIRE,
        LIBELLE_SAVOIR_FAIRE,
        CODE_MODE_EXERCICE,
        LIBELLE_MODE_EXERCICE,
        NUMERO_SIRET_SITE,
        NUMERO_SIREN_SITE,
        NUMERO_FINESS_SITE,
        NUMERO_FINESS_ETABLISSEMENT_JURIDIQUE,
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE,
        RAISON_SOCIALE_SITE,
        ENSEIGNE_COMMERCIALE_SITE,
        COMPLEMENT_DESTINATAIRE_COORD_STRUCTURE,
        COMPLEMENT_POINT_GEOGRAPHIQUE_COORD_STRUCTURE,
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
        ADRESSE_E_MAIL_COORD_STRUCTURE,
        CODE_DEPARTEMENT_STRUCTURE,
        LIBELLE_DEPARTEMENT_STRUCTURE,
        ANCIEN_IDENTIFIANT_DE_LA_STRUCTURE,
        AUTORITE_D_ENREGISTREMENT,
        CODE_SECTEUR_D_ACTIVITE,
        LIBELLE_SECTEUR_D_ACTIVITE,
        CODE_SECTION_TABLEAU_PHARMACIENS,
        LIBELLE_SECTION_TABLEAU_PHARMACIENS,
        UNNAMED_52,
        CODE_TYPE_DIPLOME_OBTENU,
        LIBELLE_TYPE_DIPLOME_OBTENU,
        CODE_DIPLOME_OBTENU,
        LIBELLE_DIPLOME_OBTENU,
        CODE_TYPE_AUTORISATION,
        LIBELLE_TYPE_AUTORISATION,
        CODE_DISCIPLINE_AUTORISATION,
        LIBELLE_DISCIPLINE_AUTORISATION,
        SPLDA_UNNAMED_13,
        SPLS_CODE_PROFESSION,
        SPLS_LIBELLE_PROFESSION,
        SPLS_CODE_CATEGORIE_PROFESSIONNELLE,
        SPLS_LIBELLE_CATEGORIE_PROFESSIONNELLE,
        SPLS_CODE_TYPE_SAVOIR_FAIRE,
        SPLS_LIBELLE_TYPE_SAVOIR_FAIRE,
        SPLS_CODE_SAVOIR_FAIRE,
        SPLS_LIBELLE_SAVOIR_FAIRE,
        SPLS_UNNAMED_13
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
        ?
    )
    """
    cursor.executemany(query_insert_F_PS, selected_sa_libreacces_rows)
    conn.commit()
    cursor.close
    conn.close


def transform_f_population (database="databse", verbose = True) :
    """
    Transformations sur les données démographiques de la population
    """
    print(" - - - Remise en forme des données démographiques de la population ...")
    conn = sqlite3.connect(
        database=database
    )
    cursor = conn.cursor()
    query_select_ref_t_popmun = """
    SELECT 
        com_code AS CCOM,
        popmun_annee AS ANNEE,
        popmun_age AS AGE,
        popmun_sexe AS SEXE,
        popmun_nb AS NB
    FROM ref_atlasante_t_popmun_com 
    WHERE com_type='COM'
    """
    cursor.execute(query_select_ref_t_popmun)
    selected_ref_t_popmun_rows = []
    while True :
        selected_ref_t_popmun_data = cursor.fetchone()
        if selected_ref_t_popmun_data :
            selected_ref_t_popmun_rows.append(tuple(selected_ref_t_popmun_data))
        else :
            break
    query_delete_f_pop = """
    DELETE FROM F_POP
    """
    cursor.execute(query_delete_f_pop)
    query_insert_F_POP = """
    INSERT INTO F_POP (
        CCOM,
        ANNEE,
        AGE,
        SEXE,
        NB
    ) VALUES (
        ?,
        ?,
        ?,
        ?,
        ?
    )
    """
    cursor.executemany(query_insert_F_POP, selected_ref_t_popmun_rows)
    conn.commit()
    cursor.close
    conn.close
    if verbose :
        print("Remise en forme des données démographiques de la population terminée")
    return


def transform_corresp_cp (database='database', verbose = True) :
    """
    Transformations sur le référentiel de correspondance code postal / code communes
    """
    print(" - - - Séparation des codes communes dans des colonnes à part ...")
    conn = sqlite3.connect(
        database=database
    )
    cursor = conn.cursor()
    query_select_ref_t_corresp_cp = """
    select
    cp_code,
    nb_com19,
    SUBSTRING(liste_com19_code,1,5) as ccom_1,
    SUBSTRING(liste_com19_code,9,5) as ccom_2,
    SUBSTRING(liste_com19_code,17,5) as ccom_3,
    SUBSTRING(liste_com19_code,25,5) as ccom_4,
    SUBSTRING(liste_com19_code,33,5) as ccom_5,
    SUBSTRING(liste_com19_code,41,5) as ccom_6,
    SUBSTRING(liste_com19_code,49,5) as ccom_7,
    SUBSTRING(liste_com19_code,57,5) as ccom_8,
    SUBSTRING(liste_com19_code,65,5) as ccom_9,
    SUBSTRING(liste_com19_code,73,5) as ccom_10,
    SUBSTRING(liste_com19_code,81,5) as ccom_11,
    SUBSTRING(liste_com19_code,89,5) as ccom_12,
    SUBSTRING(liste_com19_code,97,5) as ccom_13,
    SUBSTRING(liste_com19_code,105,5) as ccom_14,
    SUBSTRING(liste_com19_code,113,5) as ccom_15,
    SUBSTRING(liste_com19_code,121,5) as ccom_16,
    SUBSTRING(liste_com19_code,129,5) as ccom_17,
    SUBSTRING(liste_com19_code,137,5) as ccom_18,
    SUBSTRING(liste_com19_code,145,5) as ccom_19,
    SUBSTRING(liste_com19_code,153,5) as ccom_20,
    SUBSTRING(liste_com19_code,161,5) as ccom_21,
    SUBSTRING(liste_com19_code,169,5) as ccom_22,
    SUBSTRING(liste_com19_code,177,5) as ccom_23,
    SUBSTRING(liste_com19_code,185,5) as ccom_24,
    SUBSTRING(liste_com19_code,193,5) as ccom_25,
    SUBSTRING(liste_com19_code,201,5) as ccom_26,
    SUBSTRING(liste_com19_code,209,5) as ccom_27,
    SUBSTRING(liste_com19_code,217,5) as ccom_28,
    SUBSTRING(liste_com19_code,225,5) as ccom_29,
    SUBSTRING(liste_com19_code,233,5) as ccom_30,
    SUBSTRING(liste_com19_code,241,5) as ccom_31,
    SUBSTRING(liste_com19_code,249,5) as ccom_32,
    SUBSTRING(liste_com19_code,257,5) as ccom_33,
    SUBSTRING(liste_com19_code,265,5) as ccom_34,
    SUBSTRING(liste_com19_code,273,5) as ccom_35,
    SUBSTRING(liste_com19_code,281,5) as ccom_36,
    SUBSTRING(liste_com19_code,289,5) as ccom_37,
    SUBSTRING(liste_com19_code,297,5) as ccom_38,
    SUBSTRING(liste_com19_code,305,5) as ccom_39,
    SUBSTRING(liste_com19_code,313,5) as ccom_40,
    SUBSTRING(liste_com19_code,321,5) as ccom_41,
    SUBSTRING(liste_com19_code,329,5) as ccom_42,
    SUBSTRING(liste_com19_code,337,5) as ccom_43,
    SUBSTRING(liste_com19_code,345,5) as ccom_44,
    SUBSTRING(liste_com19_code,353,5) as ccom_45,
    SUBSTRING(liste_com19_code,361,5) as ccom_46
    from ref_atlasante_t_corresp_cp ratcc ;
    """
    cursor.execute(query_select_ref_t_corresp_cp)
    selected_ref_t_corresp_cp_rows = []
    while True :
        selected_ref_t_corresp_cp_data = cursor.fetchone()
        if selected_ref_t_corresp_cp_data:
            selected_ref_t_corresp_cp_rows.append(tuple(selected_ref_t_corresp_cp_data))
        else:
            break
    query_delete_TMP_CORRESP_CP = """
    DELETE FROM TMP_CORRESP_CP;
    """
    cursor.execute(query_delete_TMP_CORRESP_CP)
    query_insert_TMP_CORRESP_CP = """
    INSERT INTO TMP_CORRESP_CP (
        cp_code,
        nb_ccom,
        ccom_1,
        ccom_2,
        ccom_3,
        ccom_4,
        ccom_5,
        ccom_6,
        ccom_7,
        ccom_8,
        ccom_9,
        ccom_10,
        ccom_11,
        ccom_12,
        ccom_13,
        ccom_14,
        ccom_15,
        ccom_16,
        ccom_17,
        ccom_18,
        ccom_19,
        ccom_20,
        ccom_21,
        ccom_22,
        ccom_23,
        ccom_24,
        ccom_25,
        ccom_26,
        ccom_27,
        ccom_28,
        ccom_29,
        ccom_30,
        ccom_31,
        ccom_32,
        ccom_33,
        ccom_34,
        ccom_35,
        ccom_36,
        ccom_37,
        ccom_38,
        ccom_39,
        ccom_40,
        ccom_41,
        ccom_42,
        ccom_43,
        ccom_44,
        ccom_45,
        ccom_46
    )
    VALUES (
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
    );
    """
    cursor.executemany(query_insert_TMP_CORRESP_CP, selected_ref_t_corresp_cp_rows)
    conn.commit()
    query_select_TMP_CORRESP_CP = """
    select 
    *
    from (
        select
        cp_code,
        ccom_1 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_2 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_3 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_4 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_5 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_6 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_7 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_8 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_9 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_10 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_11 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_12 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_13 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_14 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_15 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_16 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_17 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_18 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_19 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_20 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_21 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_22 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_23 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_24 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_25 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_26 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_27 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_28 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_29 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_30 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_31 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_32 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_33 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_34 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_35 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_36 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_37 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_38 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_39 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_40 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_41 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_42 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_43 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_44 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_45 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
        union
        select
        cp_code,
        ccom_46 as ccom,
        nb_ccom
        from TMP_CORRESP_CP
    ) tout
    where ccom >0
    order by 
    cp_code,
    ccom
    """
    cursor.execute(query_select_TMP_CORRESP_CP)
    selected_TMP_CORRESP_CP_rows = []
    while True :
        selected_TMP_CORRESP_CP_data = cursor.fetchone()
        if selected_TMP_CORRESP_CP_data:
            selected_TMP_CORRESP_CP_rows.append(tuple(selected_TMP_CORRESP_CP_data))
        else:
            break
    query_delete_LI_CP_CCOM = """
    DELETE FROM LI_CP_CCOM
    """
    cursor.execute(query_delete_LI_CP_CCOM)
    query_insert_LI_CP_CCOM = """
    INSERT INTO LI_CP_CCOM (
        CP,
        CCOM,
        NB_CCOM
    )
    VALUES (
        ?,
        ?,
        ?
    );
    """
    cursor.executemany(query_insert_LI_CP_CCOM, selected_TMP_CORRESP_CP_rows)
    conn.commit()
    cursor.close
    conn.close
    if verbose :
        print("Séparation des codes communes dans des colonnes à part terminée.")
    return


