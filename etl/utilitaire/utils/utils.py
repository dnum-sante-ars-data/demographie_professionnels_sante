# coding: utf-8

# retourne la config d'un serveur au format dictionnaire
def read_config_sftp(path_in, server_name) :
    with open(path_in) as f:
        dict_ret = json.load(f)
    L_ret = dict_ret["sftp"]
    server_config = {}
    for server in L_ret :
        if server["server"] == server_name :
            server_config = server.copy()
    logging.info("Lecture config SFTP " + path_in + ".")
    return server_config

# retourne le path souhaité
def read_path(path_in, folder_name):
    with open(path_in) as f:
        dict_ret = json.load(f)
    if folder_name[:4] == "sftp":
        L_ret = dict_ret["path_sftp"]
    elif folder_name[:2] == "os":
        L_ret = dict_ret["path_os"]
    
    path_config = {}
    
    for folder in L_ret:
        if folder["folder"] == folder_name:
            path_config = folder.copy()

    logging.info("Lecture path" + path_in + ".")
    
    return path_config
