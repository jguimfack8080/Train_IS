# Documentation Technique Approfondie : Système de Prédiction des Retards (Train_IS)

## 1. Vue d'Ensemble du Système

Le système de prédiction Train_IS est une architecture de type **"Online Learning"** (Apprentissage Continu). Il ne se contente pas d'analyser le passé, mais met à jour son intelligence en permanence pour s'adapter aux conditions changeantes du réseau ferroviaire (météo, travaux, incidents).

### Composants Clés
1.  **Le Cerveau (Modèle LSTM)** : Un réseau de neurones artificiels capable de comprendre les séquences temporelles.
2.  **L'Entraîneur (DAG `train_delay_lstm`)** : Le processus qui ré-apprend les leçons récentes toutes les heures.
3.  **Le Devin (DAG `predict_future_delays`)** : Le processus qui applique ces leçons pour prédire l'avenir.

---

## 2. Le Modèle : Logique et Fonctionnement

### Pourquoi un LSTM (Long Short-Term Memory) ?
Contrairement à une régression classique qui regarde une photo instantanée, le LSTM regarde le **film** des événements.
*   **Problème :** Un retard de 5 minutes n'a pas la même signification s'il est stable depuis 1 heure ou s'il vient de bondir de 0 à 5 minutes en 10 minutes.
*   **Solution :** Le LSTM garde en mémoire les états précédents (la "mémoire à court terme") pour comprendre la *dynamique* du retard.

### Les Features (Ce que le modèle "voit")
Pour chaque train, le modèle analyse une séquence des 5 dernières étapes. À chaque étape, il observe :
*   **Le Retard Actuel :** La variable cible principale.
*   **Le Contexte Temporel :** Heure de la journée (pointe vs creuse), Jour de la semaine.
*   **Le Contexte Météo :** Pluie, Vent, Température (facteurs externes majeurs).
*   **L'Identité du Train :** Type de matériel (ICE, RE, RB) et Gare actuelle.

---

## 3. Architecture d'Orchestration (Airflow)

L'intelligence du système repose sur deux cycles de vie distincts orchestrés par Apache Airflow.

### A. Cycle d'Apprentissage (Training Pipeline)
*   **DAG :** `train_delay_lstm`
*   **Fréquence :** **Toutes les heures (`0 * * * *`)**
*   **Logique :**
    1.  **Extraction :** Récupère l'historique des **24 dernières heures** de trafic réel depuis le Data Warehouse.
    2.  **Prétraitement :** Nettoie les données, gère les valeurs manquantes, et normalise les échelles.
    3.  **Entraînement :** Le modèle ajuste ses poids synaptiques pour minimiser l'erreur entre ses prédictions et la réalité observée sur ces 24h.
    4.  **Publication :** Le nouveau "cerveau" (`model.pth`) est sauvegardé et devient immédiatement la référence pour tout le système.

*Pourquoi 1 heure ?*
Un intervalle d'une heure offre le meilleur compromis entre fraîcheur de l'information (adaptation aux tendances du jour) et stabilité/coût de calcul.

### B. Cycle de Prédiction (Inference Pipeline)
*   **DAG :** `predict_future_delays`
*   **Fréquence :** Toutes les 10 minutes
*   **Logique :**
    1.  **Chargement :** Charge la dernière version disponible du modèle (`model.pth`).
    2.  **Projection :** Récupère les horaires *théoriques* des trains à venir dans les prochaines heures.
    3.  **Simulation :** Pour chaque train futur, le modèle simule son comportement probable en se basant sur la météo prévue et la dynamique actuelle du réseau.
    4.  **Stockage :** Les prédictions sont écrites en base de données (`dwh.predictions`) pour être consommées par le Dashboard.

---

## 4. Intégration dans le Dashboard

Le Dashboard est le consommateur final de cette intelligence.
*   Il n'effectue aucun calcul complexe lui-même.
*   Il interroge simplement la table `dwh.predictions` qui est constamment alimentée par le cycle de prédiction.
*   Cela garantit une **fluidité maximale** pour l'utilisateur, car l'intelligence artificielle travaille en arrière-plan, de manière asynchrone.

---

## 5. Résumé des Flux de Données

```mermaid
graph TD
    A[Trafic Réel (FCHG)] -->|ETL| B(Data Warehouse)
    C[Météo (Open-Meteo)] -->|ETL| B
    
    B -->|Dernières 24h| D[DAG Entraînement (1h)]
    D -->|Génère| E[Fichier Modèle (model.pth)]
    
    B -->|Horaires Futurs| F[DAG Prédiction (10min)]
    E -->|Utilisé par| F
    F -->|Stocke| G[Table Prédictions]
    
    G -->|Affiche| H[Dashboard Streamlit]
```
