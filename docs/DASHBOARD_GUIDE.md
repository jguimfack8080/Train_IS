# Guide d'Interprétation du Dashboard : Bremen Train Delay Intelligence

Ce document vous guide dans la lecture et l'interprétation du Dashboard de prédiction des retards. Ce tableau de bord est conçu pour offrir une vue synthétique de l'état du réseau et des risques à venir.

---

## 1. Vue d'Ensemble

Le dashboard est divisé en quatre zones principales :
1.  **Filtres (Sidebar)** : Pour cibler l'analyse.
2.  **Indicateurs Clés (KPIs)** : État de santé global du trafic *passé/récent*.
3.  **Prédictions (Zone "Intelligence")** : Anticipation des retards futurs par le modèle IA.
4.  **Analyses Visuelles** : Graphiques pour comprendre les tendances et causes.

---

## 2. Indicateurs Clés (KPIs Historiques)

Situés en haut de page, ces chiffres résument la performance sur les **2000 derniers trains** enregistrés :

*   **Retard Moyen** : La moyenne globale des minutes de retard. Si ce chiffre augmente, le réseau se dégrade.
*   **Retard Max** : Le pire retard enregistré récemment. Utile pour repérer les incidents majeurs.
*   **Taux d'Annulation** : Pourcentage de trains annulés.
*   **Retard sous Pluie** : Moyenne des retards *uniquement* quand il pleut. Comparez ce chiffre au "Retard Moyen" pour voir si la météo impacte le trafic aujourd'hui.

---

## 3. Zone de Prédictions (L'IA en action)

C'est la section la plus importante pour l'anticipation (`🔮 Prédictions de Retard (LSTM)`).

### Les Indicateurs de Risque
*   **Trains à Risque Critique** : Nombre de trains futurs dont le retard prédit est jugé sévère (ex: > 15 min). **Action requise : Surveillance prioritaire.**
*   **Retards Possibles** : Nombre de trains avec un risque modéré.
*   **Dernière MAJ** : Heure à laquelle le modèle a généré ces prévisions. **Le système actualise les prédictions automatiquement toutes les 15 minutes.**

### Le Tableau de Prévision
Ce tableau liste les prochains trains avec leur risque associé :

| Colonne | Signification |
| :--- | :--- |
| **scheduled_time** | L'heure de départ prévue. |
| **predicted_delay_min** | L'estimation du retard en minutes par l'IA (ex: `5.4 min`). |
| **prediction_proba_class** | La catégorie de risque, avec un code couleur : |

*   🔴 **CRITICAL** (Rouge) : Retard important très probable.
*   🟠 **POSSIBLE** (Orange) : Risque de retard modéré.
*   🟢 **NO_DELAY** (Vert) : Trafic fluide prévu.

---

## 4. Analyses Visuelles

Ces graphiques aident à comprendre le "pourquoi" et le "comment" :

*   **Distribution des Retards** : Montre si la majorité des trains sont à l'heure (pic à gauche) ou si les retards sont fréquents et étalés.
*   **Évolution Temporelle** : Chaque point est un train. Permet de voir si les retards s'accumulent à certaines heures de la journée (heures de pointe).
*   **Corrélation (Météo)** : Une "carte de chaleur" pour voir les liens.
    *   Si la case croisant `precipitation` et `current_delay` est rouge foncé (proche de 1.0), la pluie cause fortement des retards.
    *   Si elle est proche de 0, la météo n'a pas d'impact actuel.

---

## 5. Comment utiliser ce Dashboard pour décider ?

1.  **Au chargement** : Regardez immédiatement les **KPIs de Prédiction**. Y a-t-il des trains en "Risque Critique" ?
2.  **Si "Risque Critique" > 0** : Descendez au tableau, identifiez les trains concernés (ID, Heure) et préparez une communication ou une action corrective.
3.  **Analyse de fond** : Si le "Retard Moyen" augmente, consultez le graphique "Évolution Temporelle" pour voir si c'est un problème ponctuel ou une dégradation continue depuis le matin.
