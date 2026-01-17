# pipeline/sources.py

SOURCES = [
    # MARCA (RSS real)
    {
        "name": "marca_portada",
        "type": "rss",
        "url": "https://e00-marca.uecdn.es/rss/portada.xml",
        "sport_hint": "mixed",  # luego filtras por keywords
    },

    # DAZN (listado HTML)
    {
        "name": "dazn_news",
        "type": "html_list",
        "url": "https://www.dazn.com/es-ES/news",
        "sport_hint": "mixed",
    },

    # EUROSPORT (listado HTML)
    {
        "name": "eurosport_mma",
        "type": "html_list",
        "url": "https://www.eurosport.es/mma/",
        "sport_hint": "mma",
    },
    {
        "name": "eurosport_ufc",
        "type": "html_list",
        "url": "https://www.eurosport.es/mma/ufc/",
        "sport_hint": "mma",
    },
]
