import sqlite3
from datetime import datetime
import pandas as pd
import pymongo
import json

# ==============================================================================
# 1. Configuration et nettoyage
# ==============================================================================
print("--- DÉBUT DE LA MIGRATION ---")

# connexions à la base de données sqlite et la bdd MongoDB
try:
    sqlite_conn = sqlite3.connect("Paris2055.sqlite")
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["Paris2055"]
    print("Connexions établies.")
except Exception as e:
    print(f"Erreur de connexion : {e}")
    exit()

# suppression anciennes collections pour repartir au propre
collections = ["Reseau", "TraficEvents", "Quartiers", "Mesures", "Horaires"]
for col in collections:
    db[col].drop()

# ==============================================================================
# 2. Préparation : Table de liaison Arret-Quartier
# ==============================================================================
print("--- Pré-traitement : Liaison Arret-Quartier ---")

# chargement des données de liaison en mémoire
df_aq = pd.read_sql_query("SELECT * FROM ArretQuartier", sqlite_conn)
map_arret_quartiers = {}

# création dictionnaire avec l'identifiant de l'arrêt en clé et l'identifiant de quartier en valeur
for _, row in df_aq.iterrows():
    aid = int(row['id_arret'])
    qid = int(row['id_quartier'])
    if aid not in map_arret_quartiers:
        map_arret_quartiers[aid] = []
    map_arret_quartiers[aid].append(qid)

print(f"Liaisons chargées ({len(map_arret_quartiers)} arrêts).")

# ==============================================================================
# 3. Collection : Quartiers (GeoJSON)
# ==============================================================================
print("--- Migration : Quartiers ---")

def parse_wkt_polygon(wkt_string):
    # conversion format wkt vers structure geojson
    try:
        if not wkt_string or not wkt_string.startswith('POLYGON'): return None
        content = wkt_string.replace("POLYGON((", "").replace("))", "")
        coordinates = []
        # parsing des coordonnées lat/lon
        for pair in content.split(","):
            parts = pair.strip().split(" ")
            coordinates.append([float(parts[0]), float(parts[1])])
        return { "type": "Polygon", "coordinates": [coordinates] }
    except:
        return None

df_quartiers = pd.read_sql_query("SELECT * FROM Quartier", sqlite_conn)
quartiers_docs = []

# construction documents quartiers
for _, row in df_quartiers.iterrows():
    doc = {
        "_id": int(row['id_quartier']),
        "nom": str(row['nom']),
        "geometry": parse_wkt_polygon(row['geojson'])
    }
    quartiers_docs.append(doc)

if quartiers_docs:
    db.Quartiers.insert_many(quartiers_docs)
    # index géospatial pour requêtes géographiques
    db.Quartiers.create_index([("geometry", "2dsphere")])
    print(f"{len(quartiers_docs)} Quartiers insérés.")

# ==============================================================================
# 4. Collection : Reseau (Lignes + Arrêts imbriqués + Véhicules)
# ==============================================================================
print("--- Migration : Reseau ---")

df_lignes = pd.read_sql_query("SELECT * FROM Ligne", sqlite_conn)
df_arrets = pd.read_sql_query("SELECT * FROM Arret", sqlite_conn)
# récupération des véhicules avec les infos chauffeur
df_vehicules = pd.read_sql_query("""
    SELECT V.*, C.nom as nom_chauffeur, C.date_embauche 
    FROM Vehicule V 
    LEFT JOIN Chauffeur C ON V.id_chauffeur = C.id_chauffeur
""", sqlite_conn)

reseau_docs = []

# itération par ligne de transport
for _, row_ligne in df_lignes.iterrows():
    id_ligne = int(row_ligne['id_ligne'])
    
    # récupération des arrêts associés à la ligne
    arrets_subset = df_arrets[df_arrets['id_ligne'] == id_ligne]
    liste_arrets = []
    for _, arr in arrets_subset.iterrows():
        id_arret = int(arr['id_arret'])
        # récupération des ids quartiers via un dictionnaire
        quartiers_ids = map_arret_quartiers.get(id_arret, [])
        
        liste_arrets.append({
            "id_arret": id_arret,
            "nom": str(arr['nom']),
            "localisation": {
                "type": "Point",
                "coordinates": [float(arr['longitude']), float(arr['latitude'])]
            },
            "quartiers_ids": quartiers_ids
        })

    # récupération des véhicules associés à la ligne
    vehicules_subset = df_vehicules[df_vehicules['id_ligne'] == id_ligne]
    liste_vehicules = []
    for _, veh in vehicules_subset.iterrows():
        liste_vehicules.append({
            "id_vehicule": int(veh['id_vehicule']),
            "immatriculation": str(veh['immatriculation']),
            "type_vehicule": str(veh['type_vehicule']),
            "capacite": int(veh['capacite']),
            "chauffeur": {
                "id": int(veh['id_chauffeur']) if pd.notnull(veh['id_chauffeur']) else None,
                "nom": str(veh['nom_chauffeur']) if pd.notnull(veh['nom_chauffeur']) else "Inconnu",
                "date_embauche": str(veh['date_embauche'])
            }
        })

    # assemblage document ligne
    doc = {
        "_id": id_ligne,
        "nom_ligne": str(row_ligne['nom_ligne']),
        "type": str(row_ligne['type']),
        "frequentation_moyenne": float(row_ligne['frequentation_moyenne']),
        "arrets": liste_arrets,
        "vehicules": liste_vehicules
    }
    reseau_docs.append(doc)

if reseau_docs:
    db.Reseau.insert_many(reseau_docs)
    print(f"{len(reseau_docs)} Lignes insérées.")

# ==============================================================================
# 5. Collection : TraficEvents (Trafic + Incidents)
# ==============================================================================
print("--- Migration : TraficEvents ---")

# jointure trafic et incidents
df_trafic = pd.read_sql_query("""
    SELECT T.*, I.id_incident, I.description, I.gravite, I.horodatage as incident_time
    FROM Trafic T
    LEFT JOIN Incident I ON T.id_trafic = I.id_trafic
""", sqlite_conn)

trafic_docs = []

# regroupement par événement trafic
for id_trafic, group in df_trafic.groupby("id_trafic"):
    first = group.iloc[0]

    try:
        trafic_time = pd.to_datetime(first['horodatage'])
    except:
        trafic_time = None
    
    doc = {
        "_id": int(first['id_trafic']),
        "id_ligne": int(first['id_ligne']),
        "horodatage": trafic_time,
        "retard_minutes": int(first['retard_minutes']),
        "evenement": str(first['evenement']),
        "incidents": []
    }
    
    # ajout liste incidents imbriqués
    for _, row in group.iterrows():
        if pd.notnull(row['id_incident']):
            try:
                inc_time = pd.to_datetime(row['incident_time'])
            except:
                inc_time = None

            doc['incidents'].append({
                "id_incident": int(row['id_incident']),
                "description": str(row['description']),
                "gravite": int(row['gravite']) if pd.notnull(row['gravite']) else 1,
                "heure": inc_time
            })
            
    trafic_docs.append(doc)

if trafic_docs:
    db.TraficEvents.insert_many(trafic_docs)
    # index sur id_ligne pour requêtes fréquentes
    db.TraficEvents.create_index("id_ligne")
    print(f"{len(trafic_docs)} Evénements trafic insérés.")

# ==============================================================================
# 6. Collection : Mesures (IoT - Capteurs)
# ==============================================================================
print("--- Migration : Mesures ---")

# récupération des mesures avec coordonnées capteur
query_mesures = """
    SELECT M.valeur, M.horodatage, M.unite, 
        C.id_capteur, C.type_capteur, C.latitude, C.longitude, C.id_arret
    FROM Mesure M
    JOIN Capteur C ON M.id_capteur = C.id_capteur
"""
df_mesures = pd.read_sql_query(query_mesures, sqlite_conn)

mesures_docs = []
for _, row in df_mesures.iterrows():
    # gestion de typage de valeur (int/float/str)
    try:
        val = float(row['valeur'])
    except:
        val = str(row['valeur'])

    doc = {
        "date": pd.to_datetime(row['horodatage']),
        "valeur": val,
        "unite": str(row['unite']),
        "type_capteur": str(row['type_capteur']),
        "id_capteur": int(row['id_capteur']),
        "id_arret": int(row['id_arret']),
        "localisation": {
            "type": "Point",
            "coordinates": [float(row['longitude']), float(row['latitude'])]
        }
    }
    mesures_docs.append(doc)

if mesures_docs:
    db.Mesures.insert_many(mesures_docs)
    # index pour requêtes géospatiales et par arrêt
    db.Mesures.create_index([("localisation", "2dsphere")])
    db.Mesures.create_index("id_arret")
    print(f"{len(mesures_docs)} Mesures insérées.")

# ==============================================================================
# 7. Collection : Horaires
# ==============================================================================
print("--- Migration : Horaires ---")

# chargement des données avec id_ligne
query_horaires = """
    SELECT H.id_horaire, H.id_arret, H.id_vehicule, H.heure_prevue, 
           H.heure_effective, H.passagers_estimes, V.id_ligne 
    FROM Horaire H
    JOIN Vehicule V ON H.id_vehicule = V.id_vehicule
"""
df_horaires = pd.read_sql_query(query_horaires, sqlite_conn)

# conversion vectorisée des dates (optimisation performance)
df_horaires['heure_prevue'] = pd.to_datetime(df_horaires['heure_prevue'], errors='coerce')
df_horaires['heure_effective'] = pd.to_datetime(df_horaires['heure_effective'], errors='coerce')

# renommage clé primaire pour mongodb
df_horaires = df_horaires.rename(columns={'id_horaire': '_id'})

# remplacement des valeurs NaT par None pour compatibilité json
df_horaires['heure_prevue'] = df_horaires['heure_prevue'].astype(object).where(df_horaires['heure_prevue'].notnull(), None)
df_horaires['heure_effective'] = df_horaires['heure_effective'].astype(object).where(df_horaires['heure_effective'].notnull(), None)

# conversion directe dataframe vers liste dictionnaires
horaires_docs = df_horaires.to_dict(orient='records')

if horaires_docs:
    db.Horaires.insert_many(horaires_docs)
    db.Horaires.create_index("id_ligne")
    print(f"{len(horaires_docs)} Horaires insérés.")

# ==============================================================================
# Rapport final de la migration de chaque collection
# ==============================================================================
print("\n--- RAPPORT ---")
# Comptage des documents par collection
for col in ["Reseau", "TraficEvents", "Quartiers", "Mesures", "Horaires"]:
    count = db[col].count_documents({})
    print(f"Collection {col:<15} : {count:>6} documents")

# Test requête géospatiale (paris centre)
test_geo = db.Quartiers.find_one({
    "geometry": {
        "$geoIntersects": {
            "$geometry": { "type": "Point", "coordinates": [2.3522, 48.8566] }
        }
    }
})
print(f"Test Geo : Trouvé '{test_geo['nom']}'" if test_geo else "Test Geo : Aucun résultat")

# fermeture des connexions
sqlite_conn.close()
client.close()
print("\nFIN DE TRAITEMENT")