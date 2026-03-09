# Mögliche Fragen und Antworten für die Projektverteidigung (Q&A)

Hier sind 10 prägnante Fragen und Antworten, die sich strikt auf die Implementierung konzentrieren.

---

## 1. Qualität & Datenintegrität

### **Frage 1: Welche Mechanismen haben Sie implementiert, um Datenduplizierung zu vermeiden?**
**Antwort:**
Ich habe einen **`event_hash`** (SHA1) eingeführt, der aus den fachlichen Daten (Zugnummer, Zeit, Bahnhof) generiert wird. Beim Laden in das Data Warehouse nutze ich den PostgreSQL-Befehl **`ON CONFLICT (event_hash) DO NOTHING`**. Das garantiert, dass jedes Ereignis nur einmal existiert. Zudem leere ich die Staging-Tabellen vor jedem Import (`TRUNCATE`), um keine Altlasten mitzuschleppen.

---

## 2. Machine Learning

### **Frage 2: Welches Modell haben Sie gewählt und warum?**
**Antwort:**
Ich verwende ein **LSTM (Long Short-Term Memory)** Netzwerk mit PyTorch. Da Zugverspätungen aufeinander aufbauen, ist dies ein klassisches Zeitreihenproblem. Ein LSTM besitzt ein „Gedächtnis“, das den Verlauf der letzten Stationen berücksichtigt, um die nächste Verspätung vorherzusagen – etwas, das einfache Regressionsmodelle nicht können.

### **Frage 3: Welche Daten nutzen Sie für das Training (Features)?**
**Antwort:**
Ich kombiniere drei Datenquellen in einer SQL-View (`v_training_dataset`):
1.  **Echtzeit-Daten:** Aktuelle Verspätung und Zugtyp.
2.  **Plan-Daten:** Die geplante Route und Kategorie.
3.  **Wetterdaten:** Temperatur und Niederschlag am jeweiligen Bahnhof (über Open-Meteo).
Zusätzlich nutze ich die Tageszeit, um verkehrsreiche Phasen abzubilden.

### **Frage 4: Wie bereiten Sie die Daten auf (Preprocessing)?**
**Antwort:**
Ich nutze einen **`RobustScaler`**, da Verspätungsdaten oft Ausreißer haben, die normale Skalierer verzerren würden. Kategorische Daten wie Bahnhofsnamen wandle ich mit einem `LabelEncoder` in Zahlen um. Wichtig ist die Sequenzierung: Ich gruppiere die Daten pro Zugfahrt und erstelle 5er-Sequenzen, damit das LSTM den zeitlichen Kontext lernt.

### **Frage 5: Wie läuft das Training technisch ab?**
**Antwort:**
Ein Python-Skript lädt die Daten der letzten 24 Stunden direkt aus der Datenbank. Ich teile sie in **80% Training** und **20% Validierung**. Als Fehlerfunktion nutze ich den **MSE (Mean Squared Error)**, da wir Minutenwerte vorhersagen. Das beste Modell wird nur gespeichert, wenn der Validierungsfehler sinkt (Early Stopping Prinzip).

### **Frage 6: Was tun Sie gegen Overfitting (Überanpassung)?**
**Antwort:**
Ich habe eine **Dropout-Layer (20%)** im LSTM implementiert. Das deaktiviert während des Trainings zufällig Neuronen und zwingt das Netz, robustere Muster zu lernen, statt die Trainingsdaten auswendig zu lernen. Zudem verwende ich separate Validierungsdaten zur Überprüfung.

---

## 3. Architektur & Prozesse

### **Frage 7: Wie sieht der Datenfluss (Pipeline) aus?**
**Antwort:**
Ich folge dem **ELT-Prinzip** (Extract, Load, Transform). Airflow zieht die Daten von der API und lädt sie roh in die **Staging-Area**. Erst danach bereinige und dedupliziere ich sie im **Data Warehouse** mittels SQL. Schließlich greift der ML-Container auf diese sauberen Daten zu, berechnet die Prognosen und speichert sie in einer `predictions`-Tabelle für das Dashboard.

### **Frage 8: Warum nutzen Sie eine SQL-View für das Training?**
**Antwort:**
Die View `v_training_dataset` abstrahiert die Komplexität. Sie führt die Joins zwischen Fahrplan- und Wetterdaten automatisch durch. Das entkoppelt meinen Python-Code von der Datenbankstruktur: Das ML-Skript muss nur `SELECT * FROM view` machen und bekommt immer das korrekte, aktuelle Format.

### **Frage 9: Welche Rolle spielt Airflow in Ihrem Projekt?**
**Antwort:**
Airflow ist mein Orchestrator. Statt unübersichtlicher Cronjobs habe ich **DAGs** definiert, die die Abhängigkeiten steuern. Zum Beispiel startet das ML-Training (`train_dag`) erst, wenn die Datenakquise erfolgreich war. Das garantiert einen stabilen, automatisierten Ablauf ohne manuelle Eingriffe.

### **Frage 10: Wie gehen Sie mit fehlenden Daten um (z. B. kein Wetter)?**
**Antwort:**
In der Datenbank nutze ich `LEFT JOINs`, um Datensätze zu erhalten, auch wenn Wetterdaten fehlen. Im Preprocessing ersetze ich diese `NULL`-Werte durch **0** (Imputation). So kann das Modell weiterarbeiten und eine Prognose basierend auf den verbleibenden Fahrplandaten abgeben, statt abzustürzen.
