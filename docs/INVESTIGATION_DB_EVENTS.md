# Investigation Technique - Données Deutsche Bahn

Ce document résume l'analyse technique des tables `dwh.timetables_fchg_events` et `dwh.timetables_rchg_events` réalisée le 08/03/2026.

## 1. Analyse de `dwh.timetables_fchg_events` (Full Change)

### Rôle de `delay_in_min`
*   **Observation** : Ce champ contient des valeurs numériques (ex: 36.0) ou `NaN`.
*   **Signification** : Il représente le retard **constaté ou estimé par la DB à l'instant T** de l'événement.
    *   C'est une donnée "temps réel" fournie par l'API IRIS (FCHG = Full Change).
    *   Ce n'est pas une prédiction de notre modèle ML, mais la "vérité terrain" ou l'estimation officielle de la DB.
*   **Utilisation Recommandée** :
    *   **Feature ML** : Doit être utilisé comme `current_delay` (retard à la gare précédente) pour prédire le retard futur.
    *   **Dashboard** : Peut être affiché comme "Retard Actuel DB" pour comparaison avec notre "Retard Prédit".

### Autres Attributs Clés
*   `is_canceled` : Booléen indiquant si le train est annulé. (Attention : parfois `NULL`, à traiter comme `FALSE`).
*   `train_name` : Nom commercial du train (ex: "RS1"). Souvent plus fiable que `train_number` pour l'affichage voyageur.

---

## 2. Analyse de `dwh.timetables_rchg_events` (Remark Change)

### Structure et Contenu
*   **Colonnes Clés** : `message_id`, `event_type` (m=message), `category` (Störung, Information), `priority` (1=High, 3=Low), `change_type` (h=hinweis/note), `train_line_name`.
*   **Exemples de Données** :
    *   "Störung" (Perturbation) valide du 09/12 au 15/12.
    *   "Information" sur des travaux ou changements de quai.

### Rôle dans l'Architecture
*   **Nature** : Ce sont des **métadonnées textuelles** et des alertes contextuelles, pas des mises à jour d'horaires brutes.
*   **Différence avec FCHG** :
    *   FCHG = "Le train aura 5 min de retard".
    *   RCHG = "Raison : Panne de signalisation" ou "Attention : Changement de quai".
*   **Utilisation Potentielle** :
    *   **Dashboard** : Afficher un bandeau d'alerte ou un tooltip si un message "Störung" concerne la gare ou la ligne affichée.
    *   **ML** : Difficile à exploiter directement (texte non structuré), sauf via NLP pour extraire des features "cause du retard".

### Conclusion
Cette table est secondaire pour le calcul pur des retards mais cruciale pour l'information voyageur (le "Pourquoi").

---

## Actions Techniques Réalisées
1.  **Dashboard** : Intégration de `is_canceled` (FCHG) pour filtrer/afficher les trains annulés.
2.  **Dashboard** : Utilisation de `route_path` (PLAN) pour déduire la vraie destination finale, remplaçant les placeholders "D" / "N".
