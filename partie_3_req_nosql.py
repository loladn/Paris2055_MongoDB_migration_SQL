import pandas as pd
import pymongo

# configuration affichage pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

print("--- REQUÊTES MONGODB (PARTIE 3) CORRIGÉES ---")

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["Paris2055"]
    print("Connexion MongoDB établie.")
except Exception as e:
    print(f"Erreur : {e}")
    exit()


# a. Moyenne des retards par ligne -> regroupement par ligne et calcul moyenne
req_a = [
    {
        "$group": {
            "_id": "$id_ligne",
            "retard_moyen": {"$avg": "$retard_minutes"}
        }
    },
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "_id",
            "foreignField": "_id",
            "as": "info_ligne"
        }
    },
    {
        "$project": {
            "id_ligne": "$_id",
            "nom_ligne": { "$first": "$info_ligne.nom_ligne" },
            "retard_moyen": 1,
            "_id": 0
        }
    },
    { "$sort": { "retard_moyen": -1 } }
]
df_a = pd.DataFrame(list(db.TraficEvents.aggregate(req_a)))
df_a = df_a[['id_ligne', 'nom_ligne', 'retard_moyen']]
df_a.to_csv("A_nosql.csv", index=False)
print("\n--- A. Moyenne retards (Top 5) ---")
print(df_a.head())


# b. Passagers moyens par jour et par ligne -> somme par jour d'abord puis moyenne globale par ligne
req_b = [
    {
        "$group": {
            "_id": {
                "ligne": "$id_ligne",
                "jour": { "$dateToString": { "format": "%Y-%m-%d", "date": "$heure_effective" } }
            },
            "total_jour": { "$sum": "$passagers_estimes" }
        }
    },
    {
        "$group": {
            "_id": "$_id.ligne",
            "passagers_moyens_par_jour": { "$avg": "$total_jour" }
        }
    },
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "_id",
            "foreignField": "_id",
            "as": "info_ligne"
        }
    },
    {
        "$project": {
            "id_ligne": "$_id",
            "nom_ligne": { "$first": "$info_ligne.nom_ligne" },
            "passagers_moyens_par_jour": 1,
            "_id": 0
        }
    },
    { "$sort": { "passagers_moyens_par_jour": -1 } }
]
df_b = pd.DataFrame(list(db.Horaires.aggregate(req_b)))
df_b = df_b[['id_ligne', 'nom_ligne', 'passagers_moyens_par_jour']]
df_b.to_csv("B_nosql.csv", index=False)
print("\n--- B. Passagers moyens/jour (Top 5) ---")
print(df_b.head())


# c. Taux d'incident sur chaque ligne -> comptage conditionnel si tableau incidents non vide
req_c = [
    {
        "$group": {
            "_id": "$id_ligne",
            "total_trafic": { "$sum": 1 },
            "nb_incidents": { "$sum": { "$size": "$incidents" } } 
        }
    },
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "_id",
            "foreignField": "_id",
            "as": "info_ligne"
        }
    },
    {
        "$project": {
            "nom_ligne": { "$first": "$info_ligne.nom_ligne" },
            "taux_incident": { "$divide": ["$nb_incidents", "$total_trafic"] },
            "_id": 0
        }
    },
    { "$sort": { "taux_incident": -1, "nom_ligne": 1 } }
]
df_c = pd.DataFrame(list(db.TraficEvents.aggregate(req_c)))
df_c.to_csv("C_nosql.csv", index=False)
print("\n--- C. Taux incident (Top 5) ---")
print(df_c.head(9)) 


# d. Emissions moyennes CO2 par véhicule -> jointure mesures co2 vers reseau pour lier aux véhicules
req_d = [
    { "$match": { "type_capteur": "CO2" } },
    
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "id_arret",
            "foreignField": "arrets.id_arret",
            "as": "ligne_info"
        }
    },
    
    { "$unwind": "$ligne_info" },
    { "$unwind": "$ligne_info.vehicules" },
    
    {
        "$group": {
            "_id": "$ligne_info.vehicules.id_vehicule",
            "emission_moyenne_CO2": { "$avg": "$valeur" }
        }
    },
    
    { "$sort": { "emission_moyenne_CO2": -1, "_id": -1 } }
]
df_d = pd.DataFrame(list(db.Mesures.aggregate(req_d)))
df_d.rename(columns={'_id': 'id_vehicule'}, inplace=True)
df_d = df_d[['id_vehicule', 'emission_moyenne_CO2']]
df_d.to_csv("D_nosql.csv", index=False)
print("\n--- D. CO2 Véhicule (Top 9) ---")
print(df_d.head(9))


# e. Top 5 quartiers nuisances sonores -> jointure mesures bruit vers reseau puis quartiers
req_e = [
    { "$match": { "type_capteur": "Bruit" } },
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "id_arret",
            "foreignField": "arrets.id_arret",
            "as": "reseau"
        }
    },
    { "$unwind": "$reseau" },
    { "$unwind": "$reseau.arrets" },
    { "$match": { "$expr": { "$eq": ["$id_arret", "$reseau.arrets.id_arret"] } } },
    { "$unwind": "$reseau.arrets.quartiers_ids" },
    {
        "$group": {
            "_id": "$reseau.arrets.quartiers_ids",
            "bruit_moyen": { "$avg": "$valeur" }
        }
    },
    {
        "$lookup": {
            "from": "Quartiers",
            "localField": "_id",
            "foreignField": "_id",
            "as": "infos"
        }
    },
    { 
        "$project": { 
            "nom": { "$first": "$infos.nom" }, 
            "bruit_moyen": 1, 
            "_id": 0 
        } 
    },
    { "$sort": { "bruit_moyen": -1 } },
    { "$limit": 5 }
]
df_e = pd.DataFrame(list(db.Mesures.aggregate(req_e)))
df_e = df_e[['nom', 'bruit_moyen']]
df_e.to_csv("E_nosql.csv", index=False)
print("\n--- E. Top Bruit Quartier ---")
print(df_e)


# f. Lignes sans incident mais retards > 10 min -> filtrage sur taille tableau incidents et retard
req_f = [
    {
        "$match": {
            "retard_minutes": { "$gt": 10 },
            "$or": [
                { "incidents": { "$exists": False } },
                { "incidents": { "$size": 0 } }
            ]
        }
    },
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "id_ligne",
            "foreignField": "_id",
            "as": "ligne"
        }
    },
    { "$project": { "nom_ligne": { "$first": "$ligne.nom_ligne" }, "_id": 0 } },
    { "$group": { "_id": "$nom_ligne" } },
    { "$project": { "nom_ligne": "$_id", "_id": 0 } },
    { "$sort": { "nom_ligne": 1 } }
]
df_f = pd.DataFrame(list(db.TraficEvents.aggregate(req_f)))
df_f.to_csv("F_nosql.csv", index=False)
print("\n--- F. Retards sans incident (Top 5) ---")
print(df_f.head())


# g. Taux de ponctualité global -> comptage total vs trajets sans retard
req_g = [
    { "$match": { "heure_effective": { "$ne": None } } },
    {
        "$group": {
            "_id": None,
            "total": { "$sum": 1 },
            "ponctuel": {
                "$sum": { 
                    "$cond": [ { "$lte": ["$heure_effective", "$heure_prevue"] }, 1, 0 ] 
                }
            }
        }
    },
    { "$project": { "taux_ponctualite": { "$divide": ["$ponctuel", "$total"] }, "_id": 0 } }
]
df_g = pd.DataFrame(list(db.Horaires.aggregate(req_g)))
df_g.to_csv("G_nosql.csv", index=False)
print("\n--- G. Ponctualité ---")
print(df_g)


# h. Nombre d’arrêts par quartier -> décompte arrêts uniques par quartier via tableau imbriqué
req_h = [
    { "$unwind": "$arrets" },
    { "$unwind": "$arrets.quartiers_ids" },
    {
        "$group": {
            "_id": "$arrets.quartiers_ids",
            "arrets_uniques": { "$addToSet": "$arrets.id_arret" }
        }
    },
    {
        "$lookup": {
            "from": "Quartiers",
            "localField": "_id",
            "foreignField": "_id",
            "as": "infos"
        }
    },
    { 
        "$project": { 
            "id_quartier": "$_id",
            "nom": { "$first": "$infos.nom" }, 
            "nombre_arrets": { "$size": "$arrets_uniques" },
            "_id": 0
        } 
    },
    { "$sort": { "nombre_arrets": -1, "id_quartier": 1 } } 
]
df_h = pd.DataFrame(list(db.Reseau.aggregate(req_h)))
df_h = df_h[['id_quartier', 'nom', 'nombre_arrets']]
df_h.to_csv("H_nosql.csv", index=False)
print("\n--- H. Arrêts par quartier (Top 9) ---")
print(df_h.head(9))


# i. Corrélation Trafic / Pollution -> calcul moyennes croisées retard et co2 par ligne
req_i = [
    {
        "$lookup": {
            "from": "TraficEvents",
            "localField": "_id",
            "foreignField": "id_ligne",
            "as": "trafic"
        }
    },
    { "$unwind": "$arrets" },
    {
        "$lookup": {
            "from": "Mesures",
            "let": { "arret_id": "$arrets.id_arret" },
            "pipeline": [
                { "$match": { "$expr": { "$eq": ["$id_arret", "$$arret_id"] }, "type_capteur": "CO2" } }
            ],
            "as": "mesures_co2"
        }
    },
    { "$unwind": { "path": "$mesures_co2", "preserveNullAndEmptyArrays": False } },
    {
        "$group": {
            "_id": "$_id",
            "nom_ligne": { "$first": "$nom_ligne" },
            "retard_moyen": { "$avg": { "$avg": "$trafic.retard_minutes" } },
            "co2_moyen": { "$avg": "$mesures_co2.valeur" }
        }
    },
    {
        "$project": {
            "id_ligne": "$_id",
            "nom_ligne": 1,
            "retard_moyen": 1,
            "co2_moyen": 1,
            "indice_correlation": { "$multiply": ["$retard_moyen", "$co2_moyen"] },
            "_id": 0
        }
    },
    { "$sort": { "indice_correlation": -1 } }
]
df_i = pd.DataFrame(list(db.Reseau.aggregate(req_i)))
df_i = df_i[['id_ligne', 'nom_ligne', 'retard_moyen', 'co2_moyen', 'indice_correlation']]
df_i.to_csv("I_nosql.csv", index=False)
print("\n--- I. Corrélation (Top 5) ---")
print(df_i.head())


# j. Moyenne de température par ligne -> filtrage capteurs temp et moyenne par ligne
req_j = [
    { "$match": { "type_capteur": { "$regex": "Temp" } } },
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "id_arret",
            "foreignField": "arrets.id_arret",
            "as": "ligne"
        }
    },
    { "$unwind": "$ligne" },
    {
        "$group": {
            "_id": "$ligne._id",
            "nom_ligne": { "$first": "$ligne.nom_ligne" },
            "temperature_moyenne": { "$avg": "$valeur" }
        }
    },
    {
        "$project": {
            "id_ligne": "$_id",
            "nom_ligne": 1,
            "temperature_moyenne": 1,
            "_id": 0
        }
    },
    { "$sort": { "temperature_moyenne": -1 } }
]
df_j = pd.DataFrame(list(db.Mesures.aggregate(req_j)))
df_j = df_j[['id_ligne', 'nom_ligne', 'temperature_moyenne']]
df_j.to_csv("J_nosql.csv", index=False)
print("\n--- J. Température Ligne (Top 5) ---")
print(df_j.head())


# k. Performance chauffeur -> liaison chauffeur véhicule vers incidents trafic
req_k = [
    { "$unwind": "$vehicules" },
    { "$match": { "vehicules.chauffeur.id": { "$ne": None } } },
    {
        "$lookup": {
            "from": "TraficEvents",
            "localField": "_id",
            "foreignField": "id_ligne",
            "as": "trafic"
        }
    },
    { "$unwind": "$trafic" },
    {
        "$group": {
            "_id": "$vehicules.chauffeur.id",
            "nom": { "$first": "$vehicules.chauffeur.nom" },
            "retard_moyen": { "$avg": "$trafic.retard_minutes" }
        }
    },
    {
        "$project": {
            "id_chauffeur": "$_id",
            "nom": 1,
            "retard_moyen": 1,
            "_id": 0
        }
    },
    { "$sort": { "retard_moyen": -1, "id_chauffeur": 1 } }
]
df_k = pd.DataFrame(list(db.Reseau.aggregate(req_k)))
df_k = df_k[['id_chauffeur', 'nom', 'retard_moyen']]
df_k.to_csv("K_nosql.csv", index=False)
print("\n--- K. Performance Chauffeur (Top 9) ---")
print(df_k.head(9))


# l. % véhicules électriques -> filtre interne au tableau véhicules pour compter les électriques
req_l = [
    {
        "$project": {
            "nom_ligne": 1,
            "total_vehicules": { "$size": "$vehicules" },
            "vehicules_elec": {
                "$filter": {
                    "input": "$vehicules",
                    "as": "v",
                    "cond": { "$eq": [ { "$toLower": "$$v.type_vehicule" }, "electrique" ] }
                }
            }
        }
    },
    {
        "$project": {
            "id_ligne": "$_id",
            "nom_ligne": 1,
            "pourcentage_electrique": { 
                "$cond": [ 
                    { "$eq": ["$total_vehicules", 0] }, 
                    0, 
                    { "$multiply": [ { "$divide": [{ "$size": "$vehicules_elec" }, "$total_vehicules"] }, 100 ] }
                ]
            },
            "_id": 0
        }
    },
    { "$sort": { "pourcentage_electrique": -1 } }
]
df_l = pd.DataFrame(list(db.Reseau.aggregate(req_l)))
df_l = df_l[['id_ligne', 'nom_ligne', 'pourcentage_electrique']]
df_l.to_csv("L_nosql.csv", index=False)
print("\n--- L. Véhicules Electriques (Top 5) ---")
print(df_l.head())


# m. Classification Qualité Service (Case When) -> case when)
# utilisation
req_m = [
    { "$match": { "type_capteur": "CO2" } },
    {
        "$group": {
            "_id": { "capteur": "$id_capteur", "arret": "$id_arret" },
            "pollution_moyenne": { "$avg": "$valeur" }
        }
    },
    {
        "$project": {
            "id_capteur": "$_id.capteur",
            "id_arret": "$_id.arret",
            "pollution_moyenne": 1,
            "niveau_pollution": {
                "$switch": {
                    "branches": [
                        { "case": { "$lt": ["$pollution_moyenne", 400] }, "then": "faible" },
                        { "case": { "$and": [ { "$gte": ["$pollution_moyenne", 400] }, { "$lte": ["$pollution_moyenne", 800] } ] }, "then": "moyenne" }
                    ],
                    "default": "elevee"
                }
            },
            "_id": 0
        }
    },
    { "$sort": { "pollution_moyenne": -1 } }
]
df_m = pd.DataFrame(list(db.Mesures.aggregate(req_m)))
df_m = df_m[['id_capteur', 'id_arret', 'pollution_moyenne', 'niveau_pollution']]
df_m.to_csv("M_nosql.csv", index=False)
print("\n--- M. Classification Pollution (Top 9) ---")
print(df_m.head(9))


# n. Qualité Service -> classification qualité service selon retard
req_n = [
    {
        "$group": {
            "_id": "$id_ligne",
            "retard_moyen": { "$avg": "$retard_minutes" }
        }
    },
    {
        "$lookup": {
            "from": "Reseau",
            "localField": "_id",
            "foreignField": "_id",
            "as": "ligne"
        }
    },
    {
        "$project": {
            "id_ligne": "$_id",
            "nom_ligne": { "$first": "$ligne.nom_ligne" },
            "retard_moyen": 1,
            "niveau_service": {
                "$switch": {
                    "branches": [
                        { "case": { "$lt": ["$retard_moyen", 7] }, "then": "OK" },
                        { "case": { "$gt": ["$retard_moyen", 7] }, "then": "ALERTE" }
                    ],
                    "default": "CRITIQUE"
                }
            },
            "_id": 0
        }
    },
    { "$sort": { "retard_moyen": -1 } }
]
df_n = pd.DataFrame(list(db.TraficEvents.aggregate(req_n)))
df_n = df_n[['id_ligne', 'nom_ligne', 'retard_moyen', 'niveau_service']]
df_n.to_csv("N_nosql.csv", index=False)
print("\n--- N. Qualité Service (Top 5) ---")
print(df_n.head())

client.close()
print("--- TERMINÉ ---")