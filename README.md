# demographie_professionnels_sante
Statistiques sur la démographie des professionnels de santé dont les données sources sont issues de l'Annuaire Santé.

## Architecture

```
_documentation
_etl
|__ _utilitaire
    |__ _data
        |__ _database
        |__ _input
        |__ _output
    |__ _log
    |__ main.py
    |__ _modules
        |__ _control
            |__ __init__.py
            |__ control.py
        |__ _gpg_decrypt
            |__ __init__.py
            |__ gpg_decrypt.py
        |__ _private_transform
            |__ __init__.py
            |__ private_transform.py
            |__ query_private_transform.py
            |__ sql_queries
        |__ _route_sftp
            |__ __init__.py
            |__ route_sftp.py
        |__ _route_sqlite
            |__ __init__.py
            |__ query_sqlite.py
            |__ route_sqlite.py
    |__ _settings
        |__ settings_demo.json
        |__ settings.json
    |__ _utils
            |__ __init__.py
            |__ utils.py
```

## Environnement
### Version de python
* python 3.9
### Modules
* argparse
* datetime
* ftplib
* gnupg
* gzip
* json
* logging
* os
* pandas
* progressbar
* pysftp
* subprocess
* sqlite3
* tqdm
* verbose
* wget


## Commandes du script d'ETL
### `import`
* Suppression des anciens fichiers .csv présents localement dans `/data/input`
* Téléchargement des fichiers sources du serveur FTP ODS `/demographie/input`en local dans `/data/input`
* Déchiffrement des fichiers importés localement de .gpg à .csv

### `init_database`
* Déploiement de la base de données et création des tables
* Dump des données depuis les fichiers sources `/data/input` et `/utils` vers la base de données locale crée précédemment

### `transform`
* Transformation des données pour créer les fichiers cibles `activites.csv` et `personnes.csv`
* Exportation des fichiers cibles dans `/data/output`

### `export_sftp`
* Publication des fichiers cibles `activites.csv` et `personnes.csv` depuis `/data/output` sur le serveur FTP dans `/demographie/output`

### `control`
* Analyse globale des fichiers cibles `activites.csv` et `personnes.csv` présents au sein de `/data/output`

### `all`
* Réalise l'ensemble des commandes définies ci-dessus


## Précautions d'emploi
* L'ensemble des paramètres des fonctions sont présents au sein du fichier `settings.json`. La fonction `read_settings()` permet de lire ces informations au sein de `main.py`.
* L'initialisation des tables de la base de données demographie_ps.db est sensible au nom des fichiers issus du dossier data/input. En cas de changement de nom des fichiers, il est nécessaire de modifier la fonction `get_column_and_table_names_for_source_files()` présente dans le fichier `/route_sqlite/query_sqlite.py` afin de faire converger le nom du fichier et le nom de la table à compléter. 
* Il en va de même avec les fichiers issus des données de l'INSEE présents dans le dossier `/utils`. En cas de changement de nom des fichiers, il est nécessaire de modifier la fonction `get_column_and_table_names_for_insee()` présente également dans le fichier `/route_sqlite/query_sqlite.py` afin de faire converger le nom du fichier et le nom de la table à compléter. 
* La fonction `import_data_from_source_files()` contient également une ligne permettant de corriger une erreur de frappe présente au sein du fichier `Extraction_RPPS_Profils1_DiplObt.csv`. En cas de changement de nom de ce fichier, il est nécessaire de faire également la modification dans la fonction en question afin de continuer à corriger cette erreur.
* Les fonctions `get_filenames_from_sftp()` et `get_filenames_from_os()` sont programmées pour retourner uniquement les fichiers .gpg ou .csv. Pour utiliser cette fonction avec des fichiers d'un autre type, il est nécessaire de modifier les fonctions dans `/utils/utils.py`.
