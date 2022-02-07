query_create_autexerc = (''' \"\"\"
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
        LIBELLE_PROFESSION               TEXT,
        UNNAMED                          TEXT
    );
   \"\"\" ''')
