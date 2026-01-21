# imports des bibliothèques principales
import streamlit as st
import pandas as pd
import pymongo
import plotly.express as px
import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
import os

# --- CONFIGURATION DE LA PAGE ---
# paramètres d'affichage streamlit
st.set_page_config(
    page_title="Paris 2055 - Supervision",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded"
)

# gestion de l'état de session pour éviter les rechargements inutiles
if 'initialized' not in st.session_state:
    st.session_state.initialized = True

# --- STYLE CSS ---
# définition du thème visuel et des couleurs
st.markdown("""
<style>
    /* Fond global blanc */
    .stApp {
        background-color: #FFFFFF;
        color: #31333F;
    }
    
    /* Style des cartes de métriques (KPI) */
    div[data-testid="metric-container"] {
        background-color: #F0F2F6;
        border: 1px solid #D6D6D6;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Titres en Bleu Marine (Style Institutionnel) */
    h1, h2, h3 { 
        color: #003366 !important; 
        font-family: 'Helvetica Neue', sans-serif;
    }
    
    /* Couleur des valeurs métriques */
    div[data-testid="stMetricValue"] { 
        color: #003366; 
    }
</style>
""", unsafe_allow_html=True)

st.title("Paris 2055 : Supervision des transports")

# --- 1. CONNEXION MONGODB ---
@st.cache_resource
def init_connection():
    """
    initialisation de la connexion à mongodb
    
    Returns:
        pymongo.MongoClient: client mongodb connecté
    """
    return pymongo.MongoClient("mongodb://localhost:27017/")

# établissement de la connexion et sélection de la base de données
try:
    client = init_connection()
    db = client["Paris2055"]
except Exception as e:
    st.error(f"Erreur de connexion MongoDB : {e}")
    st.stop()

# --- 2. PIPELINES D'AGRÉGATION (Pour les onglets Graphiques et Carto) ---

@st.cache_data(ttl=3600)
def get_kpis():
    """
    calcul des indicateurs clés de performance (kpi)
    
    Returns:
        tuple: (nombre de lignes, total incidents, valeur moyenne co2)
    """
    nb_lignes = db.Reseau.count_documents({})
    
    # agrégation pour compter le nombre total d'incidents
    nb_incidents = db.TraficEvents.aggregate([
        {"$project": {"nb_incidents": {"$size": {"$ifNull": ["$incidents", []]}}}},
        {"$group": {"_id": None, "total": {"$sum": "$nb_incidents"}}}
    ])
    res_inc = list(nb_incidents)
    total_incidents = res_inc[0]['total'] if res_inc else 0
    
    # calcul de la moyenne des mesures de co2
    avg_co2 = list(db.Mesures.aggregate([
        {"$match": {"type_capteur": "CO2"}},
        {"$limit": 1000},
        {"$group": {"_id": None, "avg": {"$avg": "$valeur"}}}
    ]))
    val_co2 = avg_co2[0]['avg'] if avg_co2 else 0
    
    return nb_lignes, total_incidents, val_co2

@st.cache_data(ttl=3600)
def get_retards_par_ligne():
    """
    récupération des retards moyens par ligne de transport
    
    Returns:
        pd.DataFrame: dataframe avec nom_ligne et retard_moyen
    """
    pipeline = [
        {"$group": {
            "_id": "$id_ligne",
            "retard_moyen": {"$avg": "$retard_minutes"}
        }},
        {"$lookup": {
            "from": "Reseau",
            "localField": "_id",
            "foreignField": "_id",
            "as": "ligne_info"
        }},
        {"$unwind": "$ligne_info"},
        {"$project": {
            "nom_ligne": "$ligne_info.nom_ligne",
            "retard_moyen": 1
        }},
        {"$sort": {"retard_moyen": -1}},
        {"$limit": 15}
    ]
    return pd.DataFrame(list(db.TraficEvents.aggregate(pipeline)))

@st.cache_data(ttl=3600)
def get_repartition_vehicules():
    """
    répartition du nombre de véhicules par type
    
    Returns:
        pd.DataFrame: dataframe avec type de véhicule et comptage
    """
    pipeline = [
        {"$unwind": "$vehicules"},
        {"$group": {
            "_id": "$vehicules.type_vehicule",
            "count": {"$sum": 1}
        }}
    ]
    return pd.DataFrame(list(db.Reseau.aggregate(pipeline)))

@st.cache_data(ttl=3600)
def get_emissions_co2_trend():
    """
    évolution temporelle des émissions de co2
    
    Returns:
        pd.DataFrame: dataframe avec date et valeur de co2
    """
    pipeline = [
        {"$match": {"type_capteur": "CO2"}},
        {"$sample": {"size": 2000}},
        {"$sort": {"date": 1}},
        {"$project": {"date": 1, "valeur": 1, "_id": 0}}
    ]
    return pd.DataFrame(list(db.Mesures.aggregate(pipeline)))

@st.cache_data(ttl=3600)
def get_arrets_data(nom_ligne_filtre=None):
    """
    données des arrêts avec statistiques environnementales
    
    Args:
        nom_ligne_filtre (str, optional): filtre sur une ligne spécifique
    
    Returns:
        pd.DataFrame: dataframe avec informations des arrêts et mesures moyennes
    """
    # construction du filtre de recherche
    match_stage = {}
    if nom_ligne_filtre and nom_ligne_filtre != "Toutes":
        match_stage = {"nom_ligne": nom_ligne_filtre}

    pipeline_arrets = [
        {"$match": match_stage},
        {"$unwind": "$arrets"},
        {"$project": {
            "id_arret": "$arrets.id_arret",
            "nom": "$arrets.nom",
            "lat": {"$arrayElemAt": ["$arrets.localisation.coordinates", 1]},
            "lon": {"$arrayElemAt": ["$arrets.localisation.coordinates", 0]},
            "_id": 0
        }},
        {"$group": {
            "_id": "$id_arret",
            "nom": {"$first": "$nom"},
            "lat": {"$first": "$lat"},
            "lon": {"$first": "$lon"},
            "lignes_desservies": {"$sum": 1}
        }}
    ]
    df_arrets = pd.DataFrame(list(db.Reseau.aggregate(pipeline_arrets)))
    
    if df_arrets.empty: return df_arrets

    # récupération des statistiques moyennes par arrêt
    pipeline_stats = [
        {"$group": {
            "_id": {"id_arret": "$id_arret", "type": "$type_capteur"},
            "moyenne": {"$avg": "$valeur"}
        }}
    ]
    stats_raw = list(db.Mesures.aggregate(pipeline_stats))
    
    # construction d'un dictionnaire pour mapper les stats par arrêt
    stats_map = {}
    for s in stats_raw:
        aid = s['_id']['id_arret']
        ctype = s['_id']['type']
        if aid not in stats_map: stats_map[aid] = {}
        stats_map[aid][ctype] = s['moyenne']

    def enrich_arret(row):
        """
        enrichissement d'une ligne d'arrêt avec les mesures environnementales
        
        Args:
            row (pd.Series): ligne contenant les infos de base de l'arrêt
        
        Returns:
            pd.Series: série avec co2, bruit et température
        """
        aid = row['_id']
        stats = stats_map.get(aid, {})
        return pd.Series({
            'CO2': stats.get('CO2', None),
            'Bruit': stats.get('db', stats.get('Bruit', None)),
            'Temp': stats.get('°C', stats.get('Temperature', None))
        })

    stats_df = df_arrets.apply(enrich_arret, axis=1)
    return pd.concat([df_arrets, stats_df], axis=1)

@st.cache_data(ttl=3600)
def get_quartiers_pollution_real():
    """
    données de pollution par quartier via agrégation
    
    Returns:
        tuple: (liste des quartiers avec géométrie, dataframe avec nom et co2 moyen)
    """
    quartiers = list(db.Quartiers.find({}, {"nom": 1, "geometry": 1, "_id": 1}))
    
    # pipeline d'agrégation pour lier mesures et quartiers via le réseau
    pipeline = [
        {"$match": {"type_capteur": "CO2"}},
        {"$lookup": {
            "from": "Reseau",
            "localField": "id_arret",
            "foreignField": "arrets.id_arret",
            "as": "reseau"
        }},
        {"$unwind": "$reseau"},
        {"$unwind": "$reseau.arrets"},
        
        {"$match": {"$expr": {"$eq": ["$id_arret", "$reseau.arrets.id_arret"]}}},
        
        {"$unwind": "$reseau.arrets.quartiers_ids"},
        
        {"$group": {
            "_id": "$reseau.arrets.quartiers_ids",
            "avg_co2": {"$avg": "$valeur"}
        }}
    ]
    
    df_res = pd.DataFrame(list(db.Mesures.aggregate(pipeline)))
    
    # construction des données pour la carte choroplèthe
    data_choropleth = []
    if not df_res.empty:
        dict_co2 = dict(zip(df_res['_id'], df_res['avg_co2']))
        
        # itération sur chaque quartier pour récupérer sa valeur de pollution
        for q in quartiers:
            val = dict_co2.get(q['_id'], 0) 
            if val > 0:
                data_choropleth.append({"nom": q['nom'], "co2": val})
    
    return quartiers, pd.DataFrame(data_choropleth)

@st.cache_data(ttl=3600)
def get_types_incidents():
    """
    top 5 des types d'incidents les plus fréquents
    
    Returns:
        pd.DataFrame: dataframe avec description d'incident et comptage
    """
    pipeline = [
        {"$match": {"incidents": {"$exists": True, "$ne": []}}},
        {"$unwind": "$incidents"},
        {"$group": {"_id": "$incidents.description", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]
    return pd.DataFrame(list(db.TraficEvents.aggregate(pipeline)))



# --- GESTION DES FICHIERS CSV ---
def get_csv_file(lettre, type_db):
    """
    chargement d'un fichier csv de résultats de requête
    
    Args:
        lettre (str): identifiant de la requête (a-n)
        type_db (str): type de base ('sql' ou 'nosql')
    
    Returns:
        pd.DataFrame or None: dataframe chargé ou none si fichier absent
    """
    # conversion en majuscule et construction du nom de fichier
    lettre_maj = lettre.upper()
    filename = f"{lettre_maj}_{type_db}.csv"
    
    if os.path.exists(filename):
        return pd.read_csv(filename)
    else:
        return None

# dictionnaire de correspondance entre identifiant et titre de requête (mapping)
REQUETES_MAP = {
    "a": "a. Moyenne des retards par ligne",
    "b": "b. Nombre moyen de passagers par ligne",
    "c": "c. Taux d'incident sur chaque ligne",
    "d": "d. Emissions moyennes de CO2 par véhicule",
    "e": "e. Top 5 quartiers nuisances sonores",
    "f": "f. Lignes sans incident mais avec retards > 10 min",
    "g": "g. Taux de ponctualité global",
    "h": "h. Nombre d'arrêts par quartier",
    "i": "i. Corrélation trafic/pollution",
    "j": "j. Moyenne température par ligne",
    "k": "k. Performance chauffeur",
    "l": "l. % véhicules électriques",
    "m": "m. Classification pollution",
    "n": "n. Retard par ligne, classé par gravité"
}


# --- 3. MISE EN PAGE ---

# affichage des indicateurs clés (kpi) en colonnes
k1, k2, k3 = st.columns(3)
lignes, incidents, co2 = get_kpis()
k1.metric("Lignes actives", lignes)
k2.metric("Incidents totaux", incidents)
k3.metric("CO2 moyen (ppm)", f"{co2:.1f}")

st.markdown("---")

# création des onglets de navigation
tab_graph, tab_map, tab_compare = st.tabs(["Analyses & Stats", "Cartographie", "Comparateur (CSV)"])

# --- ONGLET 1 : GRAPHIQUES ---
with tab_graph:
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("Retards moyens par ligne")
        df_retard = get_retards_par_ligne()
        if not df_retard.empty:
            fig = px.bar(df_retard, x="nom_ligne", y="retard_moyen", 
                         labels={"retard_moyen": "Minutes"},
                         color_discrete_sequence=["#003366"])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de données de retard.")
            
    with c2:
        st.subheader("Répartition véhicules (par type)")
        df_veh = get_repartition_vehicules()
        if not df_veh.empty:
            fig = px.pie(df_veh, values="count", names="_id", hole=0.4, 
                         color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig, use_container_width=True)

    # deuxième ligne de graphiques
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Types d'incidents fréquents")
        df_inc = get_types_incidents()
        if not df_inc.empty:
            fig_inc = px.bar(df_inc, x="count", y="_id", orientation='h', 
                             labels={"_id": "Cause", "count": "Nombre"},
                             color_discrete_sequence=["#FF4B4B"])
            st.plotly_chart(fig_inc, use_container_width=True)


    with c4:
        st.subheader("Évolution CO2 (capteurs)")
        df_co2 = get_emissions_co2_trend()
        if not df_co2.empty:
            fig_line = px.line(df_co2, x="date", y="valeur", title="Relevés CO2 bruts")
            fig_line.update_traces(line_color="#003366") 
            st.plotly_chart(fig_line, use_container_width=True)

    

# --- ONGLET 2 : CARTES ---
with tab_map:
    # sélecteur de ligne pour filtrage des arrêts
    lignes_dispo = ["Toutes"] + sorted(db.Reseau.distinct("nom_ligne"))
    choix_ligne = st.selectbox("Filtrer les arrêts par ligne :", lignes_dispo)
    
    col_map1, col_map2 = st.columns(2)
    
    # --- carte 1 : visualisation des arrêts avec indicateurs ---
    with col_map1:
        st.markdown("### Arrêts & Indicateurs")
        df_arrets = get_arrets_data(choix_ligne)
        
        if not df_arrets.empty:
            m1 = folium.Map(location=[48.8566, 2.3522], zoom_start=12, tiles="OpenStreetMap")
            marker_cluster = MarkerCluster().add_to(m1)
            
            # itération sur chaque arrêt pour création des marqueurs
            for _, row in df_arrets.iterrows():
                # gestion des valeurs manquantes dans les mesures
                txt_co2 = f"{row['CO2']:.0f} ppm" if pd.notnull(row['CO2']) else "N/A"
                txt_bruit = f"{row['Bruit']:.0f} dB" if pd.notnull(row['Bruit']) else "N/A"
                txt_temp = f"{row['Temp']:.1f} °C" if pd.notnull(row['Temp']) else "N/A"
                
                # détermination de la couleur du marqueur selon le niveau de co2
                if pd.notnull(row['CO2']):
                    val = float(row['CO2'])
                    color = "green" if val < 400 else "orange" if val < 600 else "red"
                else:
                    color = "lightgray"

                nb_lignes = int(row['lignes_desservies'])
                
                popup_txt = f"""
                <div style="font-family: sans-serif; width: 150px;">
                    <b>{row['nom']}</b><br>
                    <hr style="margin: 5px 0;">
                    Nombre lignes : {nb_lignes}<br>
                    CO2 : {txt_co2}<br>
                    Bruit : {txt_bruit}<br>
                    Temp : {txt_temp}
                </div>
                """
                
                folium.Marker(
                    location=[row['lat'], row['lon']],
                    popup=folium.Popup(popup_txt, max_width=200),
                    icon=folium.Icon(color=color, icon="info-sign")
                ).add_to(marker_cluster)
            
            st_folium(m1, width=None, height=500)

            with st.expander(f"Voir le détail des arrêts ({len(df_arrets)})", expanded=False):
                st.dataframe(
                    df_arrets[['nom', 'lignes_desservies', 'CO2', 'Bruit', 'Temp']],
                    use_container_width=True
                )
        else:
            st.warning("Aucun arrêt trouvé pour cette sélection.")

    # --- carte 2 : choroplèthe de pollution par quartier ---
    with col_map2:
        st.markdown("### Pollution par Quartier (CO2)")
        
        quartiers_geo, df_choro = get_quartiers_pollution_real()
        
        # construction du geojson pour la carte
        geo_data = {
            "type": "FeatureCollection",
            "features": []
        }
        # conversion des quartiers en features geojson
        for q in quartiers_geo:
            if q.get('geometry'):
                feature = {
                    "type": "Feature",
                    "properties": {"nom": q['nom']},
                    "geometry": q['geometry']
                }
                geo_data['features'].append(feature)
        
        if geo_data['features']:
            m2 = folium.Map(location=[48.8566, 2.3522], zoom_start=12, tiles="OpenStreetMap")
            
            folium.Choropleth(
                geo_data=geo_data,
                name="choropleth",
                data=df_choro,
                columns=["nom", "co2"],
                key_on="feature.properties.nom",
                fill_color="YlOrRd",
                fill_opacity=0.7,
                line_opacity=0.2,
                legend_name="Niveau CO2 Moyen"
            ).add_to(m2)
            
            st_folium(m2, width=None, height=500)
        else:
            st.error("Données géographiques invalides.")

# --- ONGLET 3 : COMPARATEUR STATIQUE ---
with tab_compare:
    st.header("Validation de la Migration (Source vs Cible)")
    st.markdown("Comparaison des résultats stockés dans les fichiers CSV.")
    
    col_sel, _ = st.columns([1, 2])
    with col_sel:
        # sélecteur de requête à comparer
        choix_titre = st.selectbox("Choisir la requête de test :", list(REQUETES_MAP.values()))
        # extraction de l'identifiant de la requête sélectionnée
        choix_lettre = [k for k, v in REQUETES_MAP.items() if v == choix_titre][0]

    st.divider()
    
    c_sql, c_nosql = st.columns(2)
    
    # --- affichage des résultats sql ---
    with c_sql:
        st.subheader("SQL (Origine)")
        df_sql = get_csv_file(choix_lettre, "sql")
        if df_sql is not None:
            st.dataframe(df_sql, use_container_width=True)
            st.success(f"Fichier chargé : {choix_lettre.upper()}_sql.csv")
        else:
            st.warning(f"Fichier '{choix_lettre.upper()}_sql.csv' manquant.")

    # --- affichage des résultats nosql ---
    with c_nosql:
        st.subheader("NoSQL (MongoDB)")
        df_nosql = get_csv_file(choix_lettre, "nosql")
        if df_nosql is not None:
            st.dataframe(df_nosql, use_container_width=True)
            st.success(f"Fichier chargé : {choix_lettre.upper()}_nosql.csv")
        else:
            st.warning(f"Fichier '{choix_lettre.upper()}_nosql.csv' manquant.")