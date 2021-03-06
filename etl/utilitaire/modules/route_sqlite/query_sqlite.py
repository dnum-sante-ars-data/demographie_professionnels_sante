# Liste contenant l'ensemble des noms des tables à créer
list_table_name = ["AUTEXERC", "ACTIVITE", "COORDACT", "COORDCORRESP", "COORDSTRUCT", "DIPLOBT", "ETATCIV", "EXERCPRO", "PERSONNE", "REFERAE", "SAVOIRFAIRE", "STRUCTURE", "INSEE_COMMUNES", "INSEE_DEPARTEMENT", "INSEE_REGION"]

def query_create_table(table_name):
    """
    Fonction appelée par init_empty_schema dans route_sqlite.py afin d'importer
    les query create table permettant de créer les tables au sein de la bdd.

    Paramètre :
        table_name : Nom de la table dont on souhaite connaitre 
                     la query create table. 
    """
    table_name = table_name.upper()

    if table_name == "AUTEXERC":
        query ="""CREATE TABLE IF NOT EXISTS AUTEXERC (
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
        LIBELLE_PROFESSION               TEXT,
        UNNAMED                          TEXT
    );"""

    elif table_name == "ACTIVITE":
        query = """CREATE TABLE IF NOT EXISTS ACTIVITE (
        TYPE_D_IDENTIFIANT_PP                    TEXT,
        IDENTIFIANT_PP                           TEXT,
        IDENTIFIANT_DE_L_ACTIVITE                TEXT,
        IDENTIFICATION_NATIONALE_PP              TEXT,
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE    TEXT,
        CODE_FONCTION                            TEXT,
        LIBELLE_FONCTION                         TEXT,
        CODE_MODE_EXERCICE                       TEXT,
        LIBELLE_MODE_EXERCICE                    TEXT,
        DATE_DE_DEBUT_ACTIVITE                   TEXT,
        DATE_DE_FIN_ACTIVITE                     TEXT,
        DATE_DE_MISE_A_JOUR_ACTIVITE             TEXT,
        CODE_REGION_EXERCICE                     TEXT,
        LIBELLE_REGION_EXERCICE                  TEXT,
        CODE_GENRE_ACTIVITE                      TEXT,
        LIBELLE_GENRE_ACTIVITE                   TEXT,
        CODE_MODIF_DE_FIN_D_ACTIVITE             TEXT,
        LIBELLE_MOTIF_DE_FIN_D_ACTIVITE          TEXT,
        CODE_SECTION_TABLEAU_PHARMACIENS         TEXT,
        LIBELLE_SECTION_TABLEAU_PHARMACIENS      TEXT,
        CODE_SOUS_SECTION_TABLEAU_PHARMACIENS    TEXT,
        LIBELLE_SOUS_SECTION_TABLEAU_PHARMACIENS TEXT,
        CODE_TYPE_ACTIVITE_LIBERALE              TEXT,
        LIBELLE_TYPE_ACTIVITE_LIBERALE           TEXT,
        CODE_STATUT_DES_PS_DU_SSA                TEXT,
        LIBELLE_STATUT_DES_PS_DU_SSA             TEXT,
        CODE_STATUT_HOSPITALIER                  TEXT,
        LIBELLE_STATUT_HOSPITALIER               TEXT,
        CODE_PROFESSION                          TEXT,
        LIBELLE_PROFESSION                       TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE           TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE        TEXT,
        UNNAMED                                  TEXT
    );"""

    elif table_name == "COORDACT":
        query = """CREATE TABLE IF NOT EXISTS COORDACT (         
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
        DATE_DE_FIN_COORD_ACTIVITE                   TEXT,
        UNNAMED                                      TEXT
    );"""

    elif table_name == "COORDCORRESP":
        query = """CREATE TABLE IF NOT EXISTS COORDCORRESP (
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
        DATE_DE_FIN_COORD_CORRESPONDANCE                   TEXT,
        UNNAMED                                            TEXT
    );"""

    elif table_name == "COORDSTRUCT":
        query = """CREATE TABLE IF NOT EXISTS COORDSTRUCT (
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
        DATE_DE_FIN_COORD_STRUCTURE                   TEXT,
        UNNAMED                                       TEXT
    );"""

    elif table_name == "DIPLOBT":
        query = """CREATE TABLE IF NOT EXISTS DIPLOBT (
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
        NUMERO_DIPLOME                     TEXT,
        UNNAMED                            TEXT
    );"""

    elif table_name == "ETATCIV":
        query = """CREATE TABLE IF NOT EXISTS ETATCIV (
        TYPE_D_IDENTIFIANT_PP          TEXT,
        IDENTIFIANT_PP                 TEXT,
        IDENTIFICATION_NATIONALE_PP    TEXT,
        CODE_STATUT_ETAT_CIVIL         TEXT,
        LIBELLE_STATUT_ETAT_CIVIL      TEXT,
        CODE_SEXE                      TEXT,
        LIBELLE_SEXE                   TEXT,
        NOM_DE_FAMILLE                 TEXT,
        PRENOMS                        TEXT,
        DATE_DE_NAISSANCE              TEXT,
        LIEU_DE_NAISSANCE              TEXT,
        DATE_DE_DECES                  TEXT,
        DATE_D_EFFET_DE_L_ETAT_CIVIL   TEXT,
        CODE_COMMUNE_DE_NAISSANCE      TEXT,
        LIBELLE_COMMUNE_DE_NAISSANCE   TEXT,
        CODE_PAYS_DE_NAISSANCE         TEXT,
        LIBELLE_PAYS_DE_NAISSANCE      TEXT,
        DATE_DE_MISE_A_JOUR_ETAT_CIVIL TEXT,
        UNNAMED                        TEXT
    );"""

    elif table_name == "EXERCPRO":
        query = """CREATE TABLE IF NOT EXISTS EXERCPRO (
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
        LIBELLE_DEPARTEMENT_1E_INSCRIPTION TEXT,
        UNNAMED                            TEXT
    );"""

    elif table_name == "PERSONNE":
        query = """CREATE TABLE IF NOT EXISTS PERSONNE (
        TYPE_D_IDENTIFIANT_PP        TEXT,
        IDENTIFIANT_PP               TEXT,
        IDENTIFICATION_NATIONALE_PP  TEXT,
        CODE_CIVILITE                TEXT,
        LIBELLE_CIVILITE             TEXT,
        NOM_D_USAGE                  TEXT,
        PRENOM_D_USAGE               TEXT,
        NATURE                       TEXT,
        DATE_D_EFFET                 TEXT,
        DATE_DE_MISE_A_JOUR_PERSONNE TEXT
    );"""

    elif table_name == "REFERAE":
        query = """CREATE TABLE IF NOT EXISTS REFERAE (
        TYPE_D_IDENTIFIANT_PP             TEXT,
        IDENTIFIANT_PP                    TEXT,
        IDENTIFICATION_NATIONALE_PP       TEXT,
        CODE_AE                           TEXT,
        LIBELLE_AE                        TEXT,
        DATE_DEBUT_INSCRIPTION            TEXT,
        DATE_FIN_INSCRIPTION              TEXT,
        DATE_DE_MISE_A_JOUR_INSCRIPTION   TEXT,
        CODE_STATUT_INSCRIPTION           TEXT,
        LIBELLE_STATUT_INSCRIPTION        TEXT,
        CODE_DEPARTEMENT_INSCRIPTION      TEXT,
        LIBELLE_DEPARTEMENT_INSCRIPTION   TEXT,
        CODE_DEPARTEMENT_ACCUEIL          TEXT,
        LIBELLE_DEPARTEMENT_ACCUEIL       TEXT,
        CODE_PROFESSION                   TEXT,
        LIBELLE_PROFESSION                TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE    TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE TEXT,
        UNNAMED                           TEXT
    );"""

    elif table_name == "SAVOIRFAIRE":
        query = """CREATE TABLE IF NOT EXISTS SAVOIRFAIRE (
        TYPE_D_IDENTIFIANT_PP             TEXT,
        IDENTIFIANT_PP                    TEXT,
        IDENTIFICATION_NATIONALE_PP       TEXT,
        CODE_SAVOIR_FAIRE                 TEXT,
        LIBELLE_SAVOIR_FAIRE              TEXT,
        CODE_TYPE_SAVOIR_FAIRE            TEXT,
        LIBELLE_TYPE_SAVOIR_FAIRE         TEXT,
        CODE_PROFESSION                   TEXT,
        LIBELLE_PROFESSION                TEXT,
        CODE_CATEGORIE_PROFESSIONNELLE    TEXT,
        LIBELLE_CATEGORIE_PROFESSIONNELLE TEXT,
        DATE_RECONNAISSANCE_SAVOIR_FAIRE  TEXT,
        DATE_DE_MISE_A_JOUR_SAVOIR_FAIRE  TEXT,
        DATE_ABANDON_SAVOIR_FAIRE         TEXT,
        UNNAMED                           TEXT
    );"""

    elif table_name == "STRUCTURE":
        query = """CREATE TABLE IF NOT EXISTS STRUCTURE (
        TYPE_DE_STRUCTURE                        TEXT,
        IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE    TEXT,
        IDENTIFICATION_NATIONALE_DE_LA_STRUCTURE TEXT,
        NUMERO_SIRET                             TEXT,
        NUMERO_SIREN                             TEXT,
        NUMERO_FINESS_ETABLISSEMENT              TEXT,
        NUMERO_FINESS_EJ                         TEXT,
        RPPS_RANG                                TEXT,
        DATE_DE_FERMETURE_STRUCTURE              TEXT,
        DATE_DE_MISE_A_JOUR_STRUCTURE            TEXT,
        CODE_CATEGORIE_JURIDIQUE                 TEXT,
        LIBELLE_CATEGORIE_JURIDIQUE              TEXT,
        CODE_SECTEUR_D_ACTIVITE                  TEXT,
        LIBELLE_SECTEUR_D_ACTIVITE               TEXT,
        RAISON_SOCIALE                           TEXT,
        ENSEIGNE_COMMERCIALE                     TEXT,
        UNNAMED                                  TEXT
    );"""

    elif table_name == "INSEE_COMMUNES":
        query = """CREATE TABLE IF NOT EXISTS INSEE_COMMUNES (
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
        COMPARENT TEXT,
        UNNAMED   TEXT
    );"""

    elif table_name == "INSEE_DEPARTEMENT":
        query = """CREATE TABLE IF NOT EXISTS INSEE_DEPARTEMENT (
        DEP      TEXT,
        REG      TEXT,
        CHEFLIEU TEXT,
        TNCC     TEXT,
        NCC      TEXT,
        NCCENR   TEXT,
        LIBELLE  TEXT
    );"""

    elif table_name == "INSEE_REGION":
        query = """CREATE TABLE IF NOT EXISTS INSEE_REGION (
        REG      TEXT,
        CHEFLIEU TEXT,
        TNCC     TEXT,
        NCC      TEXT,
        NCCENR   TEXT,
        LIBELLE  TEXT
    );"""
 
    else:
        print(" --- Table inconnue --- ")
    return query


def query_drop_index():
    query = """DROP INDEX IF EXISTS ACTIVITE_IDENTIFIANT_PP;
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
    DROP INDEX IF EXISTS STRUCTURE_ACTIVITE_IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE;"""
    return query


# Test create index
def query_create_index():
    query_create = ["CREATE INDEX ACTIVITE_IDENTIFIANT_PP on ACTIVITE(IDENTIFIANT_PP);", "CREATE INDEX ACTIVITE_IDENTIFIANT_DE_L_ACTIVITE on ACTIVITE(IDENTIFIANT_DE_L_ACTIVITE);", "CREATE INDEX AUTORISATION_EXERCICE_IDENTIFIANT_PP on AUTEXERC(IDENTIFIANT_PP);", "CREATE INDEX COORDONNEES_ACTIVITE_IDENTIFIANT_DE_L_ACTIVITE on COORDACT(IDENTIFIANT_DE_L_ACTIVITE);", "CREATE INDEX COORDONNEES_CORRESPONDANCE_IDENTIFIANT_PP on COORDCORRESP(IDENTIFIANT_PP);", "CREATE INDEX COORDONNEES_STRUCTURE_IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE on COORDSTRUCT(IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE);", "CREATE INDEX DIPLOME_OBTENU_IDENTIFIANT_PP on DIPLOBT(IDENTIFIANT_PP);", "CREATE INDEX ETAT_CIVIL_IDENTIFIANT_PP on ETATCIV(IDENTIFIANT_PP);", "CREATE INDEX EXERCICE_PROFESSIONNEL_IDENTIFIANT_PP on EXERCPRO(IDENTIFIANT_PP);", "CREATE INDEX INSCRIPTION_ORDRE_IDENTIFIANT_PP on REFERAE(IDENTIFIANT_PP);", "CREATE INDEX PERSONNE_IDENTIFIANT_PP on PERSONNE(IDENTIFIANT_PP);", "CREATE INDEX SAVOIR_FAIRE_IDENTIFIANT_PP on SAVOIRFAIRE(IDENTIFIANT_PP);", "CREATE INDEX STRUCTURE_ACTIVITE_IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE on STRUCTURE(IDENTIFIANT_TECHNIQUE_DE_LA_STRUCTURE);"]
    return query_create


# Récupération du nom des colonnes et de la table à compléter
def get_column_and_table_names_for_insee(files):             
    """
    Fonction appelée dans insert_data_from_insee afin de récupérer le nom de 
    la table et des colonnes cibles.
    """
    if files[:7] == "commune":
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
        'COMPARENT',
        'UNNAMED'
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

