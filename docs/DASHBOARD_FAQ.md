# FAQ Technique & Analyse du Dashboard Bremen Train Delay Intelligence

Ce document répond aux questions techniques et fonctionnelles sur l'architecture, les données et les modèles du dashboard.

---

## 1️⃣ Données et Fréquence d'Actualisation

### Quelle est la fréquence réelle des flux de données ?

Le système utilise une architecture hybride avec des fréquences différenciées selon la volatilité des données :

| Flux | Fréquence | Rôle & Justification |
| :--- | :--- | :--- |
| **Trafic Réel (FCHG)** | **10 minutes** | Capte les "changements complets" (retards, annulations, changements de quai). Fréquence élevée pour suivre la dynamique du réseau. |
| **Trafic Delta (RCHG)** | **10 minutes** | Capte les "changements récents" (delta). Permet de détecter l'apparition soudaine d'un retard. |
| **Plan de Transport (PLAN)** | **Quotidien (01:00)** | Charge les horaires théoriques des 30 prochaines heures. Sert de référence de base. |
| **Météo (Forecast)** | **6 heures** | Prévisions horaires (Open-Meteo). La météo évolue moins vite que les trains; 6h est un compromis optimal coût/fraîcheur. |
| **Météo (Historique)** | **Quotidien (J-2)** | Consolidation des données observées pour l'entraînement du modèle (lag de 48h pour qualité garantie). |

### Prise en compte dans les KPIs et Prédictions

*   **KPIs Dashboard :** Ils sont calculés à la volée (`SELECT ... FROM dwh.v_training_dataset`) lors du chargement de la page. Si vous rafraîchissez le dashboard, vous voyez les données ingérées lors du dernier run de 10 minutes.
*   **Latence :** Il existe une latence incompressible de **~10-15 minutes** entre l'événement réel (sur le quai) et son affichage (Collecte API -> Ingestion STG -> Transformation DWH -> Affichage).
*   **Données Partielles :** Les pipelines gèrent les données manquantes. Par exemple, si la météo est manquante pour une heure précise, le modèle utilise la dernière valeur connue ou une valeur par défaut (imputation).

### Recalcul du Modèle LSTM

*   **Entraînement (Training) :** Le modèle est ré-entraîné **chaque semaine** (via Airflow DAG `train_delay_lstm`) sur l'intégralité du dataset historique (plusieurs millions de lignes) pour capter les tendances à long terme.
*   **Inférence (Prédiction) :** Les prédictions sont générées **toutes les 15 minutes** (via Airflow DAG `predict_delay_lstm`) sur les données fraîches, garantissant un affichage "Live".

---

## 2️⃣ KPIs Historiques (Compréhension & Améliorations)

### Périmètre des KPIs actuels

Actuellement, la section "Performance Historique" du dashboard charge les **2000 derniers événements** pour offrir une vision rapide de l'état du réseau sans surcharger le navigateur. Cela inclut :
1.  **Trains passés ET futurs proches :** La vue contient à la fois l'historique récent et le planifié immédiat.
2.  **Trains Annulés :** Le KPI "Taux d'Annulation" (`cancel_rate`) utilise explicitement la colonne `is_canceled`.
3.  **Retards Corrigés :** Le "Retard Moyen" utilise `current_delay` qui est mis à jour par les flux FCHG/RCHG.

### Axes d'Amélioration (KPIs)

*   **Horizons Temporels :** Actuellement, le dashboard affiche une moyenne globale sur le dataset chargé.
    *   *Recommandation :* Ajouter des sélecteurs ou des mini-graphiques (Sparklines) pour voir la tendance sur 1h, 6h, 24h.
*   **KPIs Météo Enrichis :**
    *   Le dashboard montre uniquement l'impact de la pluie (`precipitation > 0`).
    *   *Pertinence :* Ajouter le **Vent** et la **Température**.

---

## 3️⃣ Prédictions du LSTM (Analyse Technique)

### Base des Prédictions (`predicted_delay_min`)

*   **Fenêtre Glissante (Sequence) :** Le LSTM utilise une séquence historique des gares traversées.
*   **Sources de Données :**
    *   ✅ **Historique Retard :** Inclus.
    *   ✅ **Météo :** Incluse.
    *   ✅ **Caractéristiques Train :** Incluses.
    *   ❌ **Incidents Réseau (Textes) :** Non traités (NLP).

### Incertitude & Intervalles de Confiance

*   **État Actuel :** Valeur unique (moyenne espérée).
*   **Incertitude :** `prediction_proba_class` indique la classe de risque.

### Trains non partis

*   Oui, intégrés via le flux **PLAN**.

---

## 4️⃣ Correctifs Techniques (Mise à jour 13.01.2026)

Suite à l'analyse des incohérences signalées, les actions correctives suivantes ont été déployées :

### Problème 1 : Données périmées et limites d'entraînement
*   **Cause Racine :** Le script d'inférence (`predict.py`) et d'entraînement (`train.py`) utilisaient des limites strictes (`limit=5000` / `limit=2000`) pour le débogage, empêchant l'utilisation du dataset complet.
*   **Correction :** Suppression totale des limites (`limit=None`). L'entraînement se fait désormais sur **l'intégralité des millions de lignes** disponibles.

### Problème 2 : Orchestration et Live Data
*   **Symptôme :** Le dashboard n'était pas "Live" et affichait "Aucune prédiction".
*   **Correction :** Mise en place de deux DAGs Airflow :
    *   `predict_delay_lstm` : S'exécute toutes les **15 minutes** pour générer des prédictions fraîches.
    *   `train_delay_lstm` : S'exécute de façon **hebdomadaire** pour mettre à jour le modèle.

### Problème 3 : Affichage Dashboard
*   **Correction :** Le dashboard filtre désormais dynamiquement les prédictions pour n'afficher que celles pertinentes pour le futur immédiat (`NOW() - 2h` jusqu'à futur), avec jointure correcte sur les noms de gares.

