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
### Import
* Suppression des anciens fichiers .csv présents localement dans /data/input
* Téléchargement des fichiers sources du serveur FTP ODS /demographie/input en local dans /data/input
* Déchiffrement des fichiers importés localement de .gpg à .csv

### Init_database
* Déploiement de la base de données et création des tables
* Dump des données depuis les fichiers sources data/input et /utils vers la base de données locale crée précédemment

### Transform 
* Création 
* Import des données  depuis le serveur FTP Atlasanté
* Déchiffrement et décompression de l'archive
* Dump des données dans une base de données en local SQLite
* Transformations sur les données
* Publication des données sur un serveur FTP

## Précautions d'emploi
* L'initialisation des tables de la base de données demographie_ps.db est sensible au nom des fichiers issus du dossier data/input. En cas de changement de nom des fichiers, il est nécessaire de modifier la fonction import_data_from_source_files() présente dans le fichier route_sqlite.py afin de faire converger le nom du fichier et le nom de la table à compléter. Il en va de même avec les fichiers issus des données de l'INSEE présents dans le dossier utils.
* La fonction import_data_from_source_files() contient également une ligne permettant de corriger une erreur de frappe présente au sein du fichier Extraction_RPPS_Profils1_DiplObt.csv. En cas de changement de nom de ce fichier, il est nécessaire de faire également la modification dans la fonction en question afin de continuer à corriger cette erreur.
