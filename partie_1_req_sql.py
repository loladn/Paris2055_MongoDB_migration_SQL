import sqlite3
import pandas

# connexion à la base de données
conn = sqlite3.connect("paris2055.sqlite")

print("--- Début de l'extraction des données ---")

# a. Moyenne des retards par ligne de transport
df_A = pandas.read_sql_query("""
    SELECT Ligne.id_ligne, Ligne.nom_ligne,
           AVG(Trafic.retard_minutes) AS retard_moyen
    FROM Trafic
    LEFT JOIN Ligne ON Trafic.id_ligne = Ligne.id_ligne
    GROUP BY Ligne.id_ligne, Ligne.nom_ligne
    ORDER BY retard_moyen DESC;
""", conn)
df_A.to_csv("A_sql.csv", index=False)
print("Requete A : OK")

# b. Nombre moyen de passagers transportés par jour et par ligne
df_B = pandas.read_sql_query("""
    WITH PassagersJour AS (
        SELECT Ligne.id_ligne,
               Ligne.nom_ligne,
               DATE(Horaire.heure_effective) AS jour,
               SUM(Horaire.passagers_estimes) AS passagers_total_jour
        FROM Horaire
        JOIN Vehicule ON Vehicule.id_vehicule = Horaire.id_vehicule
        JOIN Ligne ON Ligne.id_ligne = Vehicule.id_ligne
        GROUP BY Ligne.id_ligne, Ligne.nom_ligne, jour
    )
    SELECT id_ligne,
           nom_ligne,
           AVG(passagers_total_jour) AS passagers_moyens_par_jour
    FROM PassagersJour
    GROUP BY id_ligne, nom_ligne
    ORDER BY passagers_moyens_par_jour DESC;
""", conn)
df_B.to_csv("B_sql.csv", index=False)
print("Requete B : OK")

# c. Taux d’incident sur chaque ligne
df_C = pandas.read_sql_query("""
    WITH stats AS (
        SELECT 
            Trafic.id_ligne,
            COUNT(Incident.id_incident) AS nbre_incidents,
            COUNT(DISTINCT Trafic.id_trafic) AS nbre_trajets
        FROM Trafic
        LEFT JOIN Incident ON Trafic.id_trafic = Incident.id_trafic
        GROUP BY Trafic.id_ligne
    )
    SELECT 
        Ligne.nom_ligne,
        (CAST(stats.nbre_incidents AS FLOAT) / stats.nbre_trajets) AS taux_incident
    FROM Ligne
    LEFT JOIN stats ON Ligne.id_ligne = stats.id_ligne
    ORDER BY taux_incident DESC;
""", conn)
df_C.to_csv("C_sql.csv", index=False)
print("Requete C : OK")

# d. Emissions moyennes de CO₂ par véhicule
df_D = pandas.read_sql_query("""
    SELECT 
        Vehicule.id_vehicule,
        AVG(Mesure.valeur) AS emission_moyenne_CO2
    FROM Mesure
    JOIN Capteur ON Mesure.id_capteur = Capteur.id_capteur
    JOIN Arret ON Capteur.id_arret = Arret.id_arret
    JOIN Ligne ON Arret.id_ligne = Ligne.id_ligne
    JOIN Vehicule ON Ligne.id_ligne = Vehicule.id_ligne
    WHERE Capteur.type_capteur = 'CO2'
    GROUP BY Vehicule.id_vehicule
    ORDER BY emission_moyenne_CO2 DESC, Vehicule.id_vehicule DESC;
""", conn)
df_D.to_csv("D_sql.csv", index=False)
print("Requete D : OK")

# e. Top 5 des quartiers avec le plus de nuisances sonores
df_E = pandas.read_sql_query("""
    SELECT Quartier.nom,
           AVG(Mesure.valeur) AS bruit_moyen
    FROM Quartier
    JOIN ArretQuartier ON Quartier.id_quartier = ArretQuartier.id_quartier
    JOIN Arret ON ArretQuartier.id_arret = Arret.id_arret
    JOIN Capteur ON Arret.id_arret = Capteur.id_arret
    JOIN Mesure ON Capteur.id_capteur = Mesure.id_capteur
    WHERE Capteur.type_capteur = 'Bruit'
    GROUP BY Quartier.nom
    ORDER BY bruit_moyen DESC
    LIMIT 5;
""", conn)
df_E.to_csv("E_sql.csv", index=False)
print("Requete E : OK")

# f. Liste des lignes sans incident mais avec retards > 10 min
df_F = pandas.read_sql_query("""
    SELECT DISTINCT Ligne.nom_ligne
    FROM Trafic
    JOIN Ligne ON Trafic.id_ligne = Ligne.id_ligne
    LEFT JOIN Incident ON Trafic.id_trafic = Incident.id_trafic
    WHERE Trafic.retard_minutes > 10
      AND Incident.id_incident IS NULL
    ORDER BY Ligne.nom_ligne;
""", conn)
df_F.to_csv("F_sql.csv", index=False)
print("Requete F : OK")

# g. Taux de ponctualité global
df_G = pandas.read_sql_query("""
    SELECT 
        SUM(CASE WHEN heure_effective <= heure_prevue THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS taux_ponctualite
    FROM Horaire
    WHERE heure_effective IS NOT NULL;
""", conn)
df_G.to_csv("G_sql.csv", index=False)
print("Requete G : OK")

# h. Nombre d’arrêts par quartier
df_H = pandas.read_sql_query("""
    SELECT Quartier.id_quartier,
           Quartier.nom,
           COUNT(DISTINCT ArretQuartier.id_arret) AS nombre_arrets
    FROM Quartier
    JOIN ArretQuartier ON ArretQuartier.id_quartier = Quartier.id_quartier
    GROUP BY Quartier.id_quartier, Quartier.nom
    ORDER BY nombre_arrets DESC;
""", conn)
df_H.to_csv("H_sql.csv", index=False)
print("Requete H : OK")

# i. Corrélation entre trafic et pollution par ligne
df_I = pandas.read_sql_query("""
    WITH Retards AS (
        SELECT Ligne.id_ligne, Ligne.nom_ligne, AVG(Trafic.retard_minutes) AS retard_moyen
        FROM Trafic
        JOIN Ligne ON Ligne.id_ligne = Trafic.id_ligne
        GROUP BY Ligne.id_ligne, Ligne.nom_ligne
    ),
    Pollution AS (
        SELECT Ligne.id_ligne, AVG(Mesure.valeur) AS co2_moyen
        FROM Mesure
        JOIN Capteur ON Capteur.id_capteur = Mesure.id_capteur
        JOIN Arret ON Arret.id_arret = Capteur.id_arret
        JOIN Ligne ON Ligne.id_ligne = Arret.id_ligne
        WHERE Capteur.type_capteur = 'CO2'
        GROUP BY Ligne.id_ligne
    )
    SELECT Retards.id_ligne,
           Retards.nom_ligne,
           Retards.retard_moyen,
           Pollution.co2_moyen,
           (Retards.retard_moyen * Pollution.co2_moyen) AS indice_correlation
    FROM Retards
    JOIN Pollution ON Pollution.id_ligne = Retards.id_ligne
    ORDER BY indice_correlation DESC;
""", conn)
df_I.to_csv("I_sql.csv", index=False)
print("Requete I : OK")

# j. Moyenne de température par ligne
df_J = pandas.read_sql_query("""
    SELECT Ligne.id_ligne,
           Ligne.nom_ligne,
           AVG(Mesure.valeur) AS temperature_moyenne
    FROM Mesure
    JOIN Capteur ON Capteur.id_capteur = Mesure.id_capteur
    JOIN Arret ON Arret.id_arret = Capteur.id_arret
    JOIN Ligne ON Ligne.id_ligne = Arret.id_ligne
    WHERE Capteur.type_capteur LIKE 'Temp%'
    GROUP BY Ligne.id_ligne, Ligne.nom_ligne
    ORDER BY temperature_moyenne DESC;
""", conn)
df_J.to_csv("J_sql.csv", index=False)
print("Requete J : OK")

# k. Performance chauffeur
df_K = pandas.read_sql_query("""
    SELECT Chauffeur.id_chauffeur,
           Chauffeur.nom,
           AVG(Trafic.retard_minutes) AS retard_moyen
    FROM Trafic
    JOIN Vehicule ON Vehicule.id_ligne = Trafic.id_ligne
    JOIN Chauffeur ON Chauffeur.id_chauffeur = Vehicule.id_chauffeur
    GROUP BY Chauffeur.id_chauffeur, Chauffeur.nom
    ORDER BY retard_moyen DESC;
""", conn)
df_K.to_csv("K_sql.csv", index=False)
print("Requete K : OK")

# l. % de véhicules électriques
df_L = pandas.read_sql_query("""
    SELECT Ligne.id_ligne,
           Ligne.nom_ligne,
           SUM(CASE WHEN LOWER(Vehicule.type_vehicule) = 'electrique' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pourcentage_electrique
    FROM Vehicule
    JOIN Ligne ON Ligne.id_ligne = Vehicule.id_ligne
    GROUP BY Ligne.id_ligne, Ligne.nom_ligne
    ORDER BY pourcentage_electrique DESC;
""", conn)
df_L.to_csv("L_sql.csv", index=False)
print("Requete L : OK")

# m. Requête CASE WHEN : Classification pollution
df_M = pandas.read_sql_query("""
    WITH Pollution AS (
        SELECT Capteur.id_capteur,
               Arret.id_arret,
               AVG(Mesure.valeur) AS pollution_moyenne
        FROM Mesure
        JOIN Capteur ON Capteur.id_capteur = Mesure.id_capteur
        JOIN Arret ON Arret.id_arret = Capteur.id_arret
        WHERE Capteur.type_capteur = 'CO2'
        GROUP BY Capteur.id_capteur, Arret.id_arret
    )
    SELECT Pollution.id_capteur,
           Pollution.id_arret,
           Pollution.pollution_moyenne,
           CASE
               WHEN pollution_moyenne < 400 THEN 'faible'
               WHEN pollution_moyenne BETWEEN 400 AND 800 THEN 'moyenne'
               ELSE 'elevee'
           END AS niveau_pollution
    FROM Pollution
    ORDER BY pollution_moyenne DESC;
""", conn)
df_M.to_csv("M_sql.csv", index=False)
print("Requete M : OK")

# N. Trouver une autre requête utilisant un case when et qui ait du sens dans ce contexte : Retard classé par gravité
df_N = pandas.read_sql_query("""SELECT Ligne.id_ligne,
       Ligne.nom_ligne,
       AVG(Trafic.retard_minutes) AS retard_moyen,
       CASE
           WHEN AVG(Trafic.retard_minutes) < 7  THEN 'OK'
           WHEN AVG(Trafic.retard_minutes) > 7 THEN 'ALERTE'
           ELSE 'CRITIQUE'
       END AS niveau_service
       FROM Trafic
       JOIN Ligne ON Ligne.id_ligne = Trafic.id_ligne
       GROUP BY Ligne.id_ligne, Ligne.nom_ligne
       ORDER BY retard_moyen DESC;""", conn)
df_N.to_csv("N_sql.csv", index=False)
print("Requete N : OK")

# Fermeture de la connexion
conn.close()
print("--- Terminé : Tous les fichiers CSV ont été générés ---")