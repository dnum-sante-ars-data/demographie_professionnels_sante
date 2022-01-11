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
<<<<<<< HEAD
        |__ _pgp_decrypt
            |__ __init__.py
            |__ _pgp_decrypt.py
        |__ _private_transform
            |__ __init__.py
            |__ _public_transform.py
            |__ sql_queries
        |__ _route_sftp
            |__ __init__.py
            |__ route_sftp.py
        |__ _route_sqlite
            |__ __init__.py
            |__ route_sqlite.py
=======
        |__ _private_transform
        |__ _public_transform
        |__ _route_sftp
        |__ _route_sqlite
            |__ __init__.py
            |__ sqlite_config.py
        |__ _route_webservice
        |__ _sql_queries
>>>>>>> b1e3ace8f433539081040ab19f45fbb427e6160e
    |__ _settings
        |__ settings.json
    |__ _utils 
```

## Environnement
### Version de python
* python 3.9
### Modules
* argparse
* logging
* datetime
* pysqlite3
* json
<<<<<<< HEAD
* wget
* ftplib
* gnupg
* gzip
=======
>>>>>>> b1e3ace8f433539081040ab19f45fbb427e6160e

## Commandes du script d'ETL
* Import des données  depuis le serveur FTP Atlasanté
* Déchiffrement et décompression de l'archive
* Dump des données dans une base de données en local SQLite
* Transformations sur les données
* Publication des données sur un serveur FTP
