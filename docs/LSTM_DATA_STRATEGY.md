# Stratégie de Données pour Modèle LSTM (Train Delay Prediction)

Ce document définit la stratégie complète pour transformer les données brutes du Data Warehouse (DWH) en un dataset d'entraînement optimisé pour un modèle Deep Learning (LSTM). Il intègre **toutes** les sources de données disponibles : Trafic (FCHG, PLAN, RCHG) et Météo (Forecast).

---

## 1. Inventaire et Analyse des Sources de Données

### A. Trafic : État Actuel (`dwh.timetables_fchg_events`)
*   **Signification Métier :** Représente l'état complet du tableau d'affichage (Arrival/Departure boards) à un instant T. C'est la source de vérité principale pour l'état du trafic en temps quasi-réel.
*   **Rôle ML :** Fournit la **Target** (retard actuel) et les **Features Dynamiques** (état du réseau).
*   **Attributs Clés :**
    *   `delay_in_min` (Target) : Le retard à prédire.
    *   `event_time` (Feature) : L'heure programmée (pivot temporel).
    *   `eva_number` (Feature) : Identifiant de la gare (contexte spatial).
    *   `train_name` / `train_type` (Feature) : Type de matériel (ICE vs RE).
*   **Transformation :**
    *   Normalisation des retards (RobustScaler).
    *   Encodage des gares (Entity Embedding).

### B. Trafic : Plan de Transport (`dwh.timetables_plan_events`)
*   **Signification Métier :** Contient le plan de transport théorique (publié à l'avance). Il représente la "promesse" faite au voyageur et la structure rigide du réseau.
*   **Rôle ML :** Fournit les **Features Statiques** et le **Contexte Structurel**.
*   **Attributs Clés :**
    *   `route_path` (Feature) : La séquence des gares prévues. Permet de reconstruire la topologie de la ligne.
    *   `train_category` (Feature) : Priorité du train (un ICE passe avant un RB).
    *   `platform` (Feature) : Quai prévu (un changement de quai `fchg` vs `plan` est un signal de perturbation).
*   **Valeur Ajoutée :** Permet au modèle de comprendre la "normalité". Un retard de 5 min est-il critique ? Sur un ICE (plan serré), oui. Sur un RE (plan lâche), non.

### C. Trafic : Changements Récents (`dwh.timetables_rchg_events`)
*   **Signification Métier :** Flux d'événements delta. Contient les modifications incrémentales (messages libres, changements de voie inopinés).
*   **Rôle ML :** Fournit la **Dynamique de Perturbation** (dérivée du retard).
*   **Attributs Clés :**
    *   `message_id` / Free Text (Feature) : Codes d'incidents (ex: "Personne sur les voies").
    *   `timestamp_event` (Feature) : Moment précis de l'annonce du retard.
*   **Transformation :**
    *   Calcul de la "volatilité" : Combien de mises à jour `rchg` dans les 30 dernières minutes ? (Indicateur de chaos).
    *   Encodage des messages fréquents.

### D. Météo : Prévisions (`dwh.v_weather_forecast_hourly`)
*   **Signification Métier :** Conditions atmosphériques prévues sur la zone géographique. Facteur exogène majeur de friction.
*   **Rôle ML :** **Variable Exogène** explicative.
*   **Attributs Clés :**
    *   `temperature_2m` (Feature) : Gel/Canicule (impact matériel).
    *   `precipitation` (Feature) : Pluie/Neige (adhérence, vitesse réduite).
    *   `wind_speed_10m` (Feature) : Tempêtes (chute d'arbres).
    *   `weather_code` (Feature) : Type de temps (WMO code).
*   **Intégration :** Jointure spatio-temporelle (Station GPS + Heure Événement).
*   **Valeur Ajoutée :** Explique les retards "systémiques" (tout le réseau ralentit).

---

## 2. Stratégie de Fusion (Data Fusion Strategy)

L'objectif est de créer une vue unique `dwh.v_training_dataset` qui aligne toutes ces sources.

### Le Concept Pivot : "Stop Event"
L'unité atomique d'apprentissage est un **Arrêt** (Arrivée ou Départ d'un train dans une gare).
*   **Clé Primaire Composite :** `(train_line_ride_id, eva_number, event_time)`

### Logique de Jointure (SQL View Logic)
1.  **Backbone (Squelette) :** `dwh.timetables_fchg_events` (Source la plus riche en temps réel).
2.  **Enrichissement Structurel (LEFT JOIN PLAN) :**
    *   On joint `timetables_plan_events` sur `train_line_ride_id` et `eva_number`.
    *   *But :* Récupérer la route théorique complète et les attributs de train fixes si manquants dans FCHG.
3.  **Enrichissement Dynamique (LEFT JOIN RCHG Aggrégé) :**
    *   On joint des métriques agrégées de `timetables_rchg_events` (ex: nombre de messages dans l'heure précédente pour ce train).
4.  **Enrichissement Contextuel (LEFT JOIN METEO) :**
    *   On joint `dwh.v_weather_forecast_hourly` sur :
        *   `geometry` (Station la plus proche -> déjà géré si on a un mapping station/météo, sinon on utilise une station météo "Bremen Central" par défaut pour ce MVP ou on joint sur coordonnées).
        *   `time` (Heure de l'événement arrondie à l'heure la plus proche).

---

## 3. Feature Engineering & Transformations

### A. Features Temporelles (Cyclical Encoding)
L'heure n'est pas linéaire (23h est proche de 00h).
*   `sin_hour = sin(2 * pi * hour / 24)`
*   `cos_hour = cos(2 * pi * hour / 24)`
*   `is_weekend` (Boolean)

### B. Features de Retard (Lag Features)
Le modèle LSTM a besoin du passé pour prédire le futur.
*   La vue SQL fournira les données "à plat".
*   Le **Tensor Builder** (Python) créera les séquences :
    *   *Input :* [Retard Gare N-5, Retard Gare N-4, ..., Retard Gare N-1]
    *   *Target :* Retard Gare N

### C. Features Météo
*   Normalisation MinMax pour Température/Vent.
*   Log-transform pour Précipitations (souvent distribuées en loi de puissance).

---

## 4. Architecture du Dataset Final (Tensor Structure)

Dimensions : `(N, L, F)`
*   **N (Samples) :** Nombre de trajets uniques
*   **L (TimeSteps) :** Longueur de la séquence (ex: 5 dernières gares)
*   **F (Features) :** Nombre de variables par pas de temps

**Vecteur de Features F_t (à l'instant t) :**
1.  `current_delay` (min) - *Source: FCHG*
2.  `scheduled_buffer_next_station` (min) - *Source: PLAN (Différence entre arrivée N et départ N)*
3.  `sin_time_of_day` / `cos_time_of_day` - *Source: FCHG*
4.  `weather_precip_mm` - *Source: METEO*
5.  `weather_wind_speed` - *Source: METEO*
6.  `rchg_message_count` - *Source: RCHG*
7.  `train_category_encoded` - *Source: PLAN/FCHG*

---

## 5. Prochaines Étapes (Implémentation)

1.  **SQL :** Créer la vue `dwh.v_training_dataset` implémentant les jointures ci-dessus.
2.  **Python :** Créer un script de chargement qui lit cette vue et construit les tenseurs Numpy.
3.  **Viz :** Explorer ces données corrélées dans un dashboard.

### 5.3. Pipeline d'Entraînement & Orchestration

1.  **Volume de Données :** L'entraînement s'effectue sur **l'intégralité du dataset historique** (plusieurs millions de lignes), sans échantillonnage restrictif (`limit=None`), pour garantir la robustesse du modèle.
2.  **Filtrage :** Garder uniquement les trajets complets (supprimer les fragments de trajets).
3.  **Imputation :** `NULL` delay -> 0.
4.  **Split :** Train (80%), Val (10%), Test (10%) respectant la chronologie.
5.  **Orchestration (Airflow) :**
    *   **Training (`train_delay_lstm`) :** Exécution **hebdomadaire** (Weekly). Ré-entraîne le modèle sur toutes les données disponibles jusqu'à J-1.
    *   **Inference (`predict_delay_lstm`) :** Exécution toutes les **15 minutes**. Génère des prédictions pour les trains des prochaines heures.

---

## 6. Implémentation Actuelle du Modèle (Production)

Le modèle est opérationnel et vérifié. Il s'agit d'un réseau de neurones récurrents **LSTM (Long Short-Term Memory)**, spécifiquement conçu pour traiter des séquences temporelles.

### 6.1. Architecture du Modèle (`model.py`)
Le modèle est léger et optimisé pour la performance (~52 000 paramètres).
*   **Entrée (Input) :** Une séquence des **5 dernières étapes** du train (Fenêtre glissante).
*   **Cœur (LSTM Layers) :**
    *   **2 couches LSTM empilées** : Permet de capturer des relations complexes (ex: accumulation de retard progressive vs incident ponctuel).
    *   **64 neurones cachés (Hidden Units)** : La "capacité de mémoire" du modèle.
    *   **Dropout (0.2)** : Désactive aléatoirement 20% des neurones pendant l'entraînement pour éviter le sur-apprentissage (overfitting).
*   **Sortie (Output) :** Une couche linéaire simple (`Linear`) qui transforme la mémoire du LSTM en **une seule valeur** : le retard prédit en minutes.

### 6.2. Les Données Traitées (`preprocessor.py`)
Pour chaque prédiction, le modèle analyse **8 caractéristiques (features)** sur les 5 derniers points de temps :
1.  **Retard actuel (`current_delay`)** : L'historique récent du retard.
2.  **Météo** : `temperature_2m`, `precipitation`, `wind_speed_10m` (Impact des conditions climatiques).
3.  **Contexte Temporel** : `hour_of_day`, `day_of_week` (Heures de pointe, week-end...).
4.  **Identité du Train** : `train_type` (ICE, RB, RE...).
5.  **Localisation** : `station_name` (Encodé numériquement via LabelEncoder).

### 6.3. Processus d'Apprentissage (`train.py`)
*   **Objectif :** Minimiser l'erreur quadratique moyenne (**MSE**) entre le retard prédit et le retard réel.
*   **Séquençage :** Il apprend par l'exemple : *"Si j'ai vu [Gare A: 0min, Gare B: 2min, Gare C: 5min...], alors à la Gare F le retard sera de X min"*.
*   **Optimiseur :** Adam (Standard pour ce type de réseau).
