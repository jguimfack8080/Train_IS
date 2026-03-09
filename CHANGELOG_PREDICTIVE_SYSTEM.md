# Changelog: Système Prédictif & Pipeline

## 1. Diagnostic du Système Existant (Avant Correction)
**Problème constaté :**
Le système affichait des données "temps réel" qui étaient en réalité des données passées (le dernier train affiché avait déjà circulé). Aucune prédiction sur les trains futurs n'était disponible.

**Causes Racines :**
1.  **Pipeline Réactif et non Proactif :** Le modèle ML n'était déclenché que sur des données d'événements *reçus* ou *passés*, agissant comme un "constat a posteriori" plutôt qu'un moteur de prévision.
2.  **Absence de Génération de Données Futures :** Il n'existait pas de processus pour interroger les horaires théoriques (Plan) des 24 prochaines heures et les soumettre au modèle.
3.  **Filtres Dashboard Inadéquats :** Le dashboard affichait simplement les dernières données disponibles sans filtrage temporel strict autour de "Maintenant".

## 2. Nouvelle Architecture Prédictive

### A. Pipeline de Génération des Prédictions (`predict_future.py`)
Un nouveau script a été créé pour rendre le système réellement prédictif.
-   **Source des Données :**
    -   **Horaires :** Table `dwh.timetables_plan_events` (Horaires théoriques planifiés).
    -   **Météo :** Table `dwh.v_weather_forecast_hourly` (Prévisions météo horaires).
    -   **Jointure :** Les trains futurs (NOW à NOW+24h) sont enrichis avec les prévisions météo correspondantes.
-   **Inférence ML :**
    -   Utilisation du modèle LSTM entraîné (`model.pth`).
    -   Génération de séquences de features pour chaque train futur.
    -   Prédiction du retard en minutes (`predicted_delay_min`).
-   **Stockage (Upsert) :**
    -   Les prédictions sont insérées dans la table `dwh.predictions`.
    -   Mécanisme **"Upsert"** (Insert on Conflict Update) implémenté pour éviter les doublons et mettre à jour les prédictions existantes si elles changent (ex: nouvelle météo).

### B. Orchestration (Airflow)
-   **DAG :** `predict_future_delays` (anciennement `predict_delay_lstm` modifié/remplacé).
-   **Fréquence :** Toutes les **10 minutes**.
-   **Mécanisme :** Le DAG déclenche l'exécution du script `python /app/predict_future.py` à l'intérieur du conteneur `ml_engine`.

### C. Visualisation (Dashboard Streamlit)
Le dashboard a été refondu pour se concentrer sur l'aide à la décision.
-   **Filtre Temporel par Défaut :** Affiche uniquement les trains entre `NOW() - 1h` et `NOW() + 24h`.
-   **Mode Historique :** Toggle "Afficher l'historique complet" pour consulter les anciennes prédictions.
-   **Indicateurs de Risque :**
    -   Scatter Plot avec taille des points proportionnelle au retard prévu.
    -   **[NOUVEAU]** Heatmap (Carte de Chaleur) : Retards moyens par Heure vs Gare.
    -   **[NOUVEAU]** Pie Chart (Diagramme Circulaire) : Répartition des niveaux de risque (High/Medium/Low).
    -   Tableau détaillé avec score de confiance et classe de risque.
    -   Alertes actionnables pour les retards > 10 min.

## 3. Validation Technique
-   **Script de prédiction :** Testé avec succès, génère ~500+ prédictions pour les 24h à venir.
-   **Base de Données :** La table `dwh.predictions` se peuple correctement avec des timestamps `scheduled_time` futurs.
-   **Dashboard :** Affiche correctement les données futures et filtre les données obsolètes.

## 4. Fichiers Modifiés/Créés
-   `ml_engine/src/predict_future.py` (Nouveau)
-   `ml_engine/src/utils/db.py` (Fonctions utilitaires BDD)
-   `visualization/app.py` (Refonte logique d'affichage)
-   `airflow/dags/predict_dag.py` (Mise à jour de l'orchestration)

## 5. Derniers Correctifs (08/03/2026)
-   **Dashboard Fix :** Correction de l'erreur `UndefinedColumn: column "train_line" does not exist` en utilisant l'alias correct `train_line_name as train_line`.
-   **Timezone Fix :** Gestion explicite des fuseaux horaires (`timezone.utc`) pour éviter les erreurs de comparaison `TypeError: Invalid comparison between dtype=datetime64[ns, UTC] and datetime`.
-   **Fonctionnalités Restaurées :** Réintégration du Heatmap (Retards par heure/gare) et du Diagramme Circulaire (Répartition des risques) dans le dashboard.

## 6. Correctifs Récents (08/03/2026 - 12h50)
-   **Problème d'Affichage (2 Gares Seulement) :**
    -   **Symptôme :** Le dashboard n'affichait que les gares `Bremen Hbf` et `Bremen-Neustadt`, alors que 17 gares sont suivies.
    -   **Cause Racine :** Le DAG `db_timetables_plan_import` (responsable de l'import des horaires théoriques) se basait sur une `logical_date` décalée, entraînant un manque de données planifiées pour la journée en cours au-delà de 06h00.
    -   **Solution :**
        -   Déclenchement manuel du DAG `db_timetables_plan_import` pour la date du jour (08/03) afin de récupérer les horaires manquants.
        -   Relance du script de prédiction `predict_future.py` pour générer les prédictions sur l'ensemble des gares.
    -   **Résultat :** Le dashboard affiche désormais les prédictions pour l'ensemble des 16-17 gares actives.

## 7. Version 1.1.0 - Corrections UI & Données Complètes (08/03/2026 - 14h00)

### A. Fix : Données Manquantes (Bremerhaven)
-   **Problème** : Les gares de Bremerhaven n'affichaient aucune donnée pour la journée en cours.
-   **Diagnostic** : L'ingestion planifiée (DAG) avait manqué les données pour ces gares spécifiques sur la fenêtre temporelle actuelle.
-   **Résolution** : Exécution d'un script d'ingestion manuelle couvrant 24h pour toutes les gares, suivi d'une régénération des prédictions.
-   **Statut** : Données rétablies pour Bremerhaven Hbf, Lehe, et Wulsdorf.

### B. Amélioration UI : Visualisation du Trajet (Timeline)
-   **Problème** : La colonne `route_path` était illisible (chaîne brute avec séparateurs `|`).
-   **Solution Implémentée** :
    1.  **Tableau Principal** : Remplacement des `|` par des flèches `→` et élargissement de la colonne avec scroll horizontal.
    2.  **Vue Détaillée (Modal)** : Implémentation d'un système de **Timeline Visuelle** interactif.
        -   Accessible via clic sur une ligne du tableau (`st.dialog`).
        -   Affichage graphique des arrêts : Départ (Vert), Arrivée (Rouge), Intermédiaires (Gris).
        -   Scroll horizontal pour les trajets longs.
        -   Design responsive et moderne (HTML/CSS injecté).

### C. Documentation Technique
-   Rédaction complète de l'architecture technique dans `docs/PREDICTIVE_SYSTEM_ARCHITECTURE.md`.
-   Couverture détaillée du modèle LSTM, des pipelines ETL Airflow, et de la logique de corrélation.

👉 **[Consulter la Documentation Complète](docs/PREDICTIVE_SYSTEM_ARCHITECTURE.md)**
