"""Config Superset pour le projet grp3."""
from flask_appbuilder.security.manager import AUTH_DB

AUTH_TYPE = AUTH_DB
SECRET_KEY = "grp3_change_me"

# Autorise les appels API REST avec Bearer token sans CSRF cookie
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = ["superset.views.core.log"]

# Requis pour que le Bearer token fonctionne depuis l'extérieur du conteneur
ENABLE_PROXY_FIX = True
SUPERSET_WEBSERVER_TIMEOUT = 300

# Autorise l'API sans origine stricte (demo locale)
HTTP_HEADERS = {"X-Frame-Options": "SAMEORIGIN"}
