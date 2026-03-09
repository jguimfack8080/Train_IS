# Rede-Skript: Informationssystem Vortrag (20 Min)

**Sprecher:** Jordan Guimfack Jeuna (Matrikelnr. 38184)
**Modul:** Informationssysteme (WiSe 2025/2026)
**Thema:** Entwicklung eines Informationssystems zur Vorhersage von Zugausfällen in Bremen auf Basis von Wetterdaten und Daten der Deutschen Bahn API
**Dauer:** 15 Min Rede + 5 Min Live-Demo

---

## [00:00 - 02:00] Einleitung & Agenda
*(Slide 1: Titel & Slide 2: Agenda)*

Sehr geehrter Herr Professor Hündling, liebe Kommilitonen.

Mein Name ist Jordan Guimfack Jeuna. Das Thema heute: Ein Informationssystem zur Vorhersage von Zugausfällen.

**(Weiter zu Slide 2: Agenda)**

Kurz zur Agenda:
Wir beginnen mit **Problem & Ziel**, schauen uns die **Architektur** an, tauchen in den **Datenfluss** und die **Qualitätssicherung** ein, erklären das **Machine Learning** und enden mit einer **Live-Demo**.

**(Weiter zu Slide 3: Der Start)**

Lassen Sie uns direkt einsteigen.

Ich möchte Sie bitten, sich kurz an eine Situation zu erinnern, die wir alle, besonders hier im Norden, nur zu gut kennen.

Es ist Winter. Sie stehen an einem Bahnsteig, vielleicht hier am Bremer Hauptbahnhof. Es ist nasskalt, der Wind pfeift. Sie schauen auf die Anzeigetafel, dann auf Ihre Uhr. Und in Ihrem Kopf formt sich diese eine, universelle Frage:

**"Kommt er noch, oder kommt er nicht?"**

Diese Frage ist nicht nur ein persönliches Ärgernis. Sie ist Ausdruck einer fundamentalen **Informationslücke**. In diesem Moment fehlen Ihnen verlässliche Daten, um eine Entscheidung zu treffen: *Warte ich weiter in der Kälte, oder nehme ich das Taxi?*

Millionen von Entscheidungen hängen täglich von dieser einen Information ab. Und genau hier setzt mein Projekt an.

---

## [02:00 - 04:00] Das Ziel & Der Ansatz
*(Slide 4: Das Ziel & Die Realität)*

Mein Ziel war es, diese Lücke zu schließen. Nicht durch eine einfache Fahrplan-App, sondern durch ein echtes **Informationssystem**. Ein System, das nicht nur passiv meldet, *dass* ein Zug zu spät ist, sondern das *versteht*, wie sich Verspätungen im Netz fortpflanzen.

Der Ansatz war ganzheitlich: Ich wollte keine isolierte "KI-Insel" bauen, sondern einen transparenten Prozess von der Datenquelle bis zur Vorhersage. Ein System, das "End-to-End" auf meinem eigenen Server läuft – also Self-Hosted und unabhängig.

Die Realität, die ich Ihnen heute vorstelle, ist ein funktionierender Prototyp. Er zeigt das Potenzial, aber er offenbart auch schonungslos die Herausforderungen, die entstehen, wenn man echte, "schmutzige" Daten aus der realen Welt in ein geordnetes System zwingen will.

---

## [04:00 - 06:00] Die Architektur
*(Slide 5: Das Big Picture)*

Lassen Sie uns auf das Fundament schauen. Ein Informationssystem ist wie ein Haus – es braucht eine solide Statik.

Sie sehen hier das Rückgrat meines Systems.
Alles beginnt links bei den Datenquellen: Der **Deutschen Bahn API** und **Open-Meteo**.
Im Zentrum steht unser Dirigent: **Apache Airflow**. Er steuert den Takt. Er entscheidet, wann welche Daten fließen.
Gespeichert wird alles in einer **PostgreSQL** Datenbank, die wir aber nicht einfach als "Ablage" nutzen, sondern in strikte Schichten unterteilt haben.
Und ganz am Ende steht die Intelligenz: Ein **LSTM-Netzwerk**, das aus der Vergangenheit lernt, um die Zukunft vorherzusagen.
Schließlich visualisieren wir diese Ergebnisse mit **Streamlit**, wie Sie im Tech-Stack unten rechts sehen können. Es ist das Fenster zum System.

Wichtig ist: Daten fließen nicht einfach von A nach B. Sie müssen geführt, gereinigt und überwacht werden. Das ist die Aufgabe dieses Systems.

---

## [06:00 - 08:30] Die Datenquellen (Detail)
*(Slide 6: Die Datenquellen)*

Lassen Sie uns einen Blick auf die Rohstoffe werfen. Wir nutzen exakt **sechs Datenquellen**, die wie Zahnräder ineinandergreifen.

**1. DB Stations:**
Das sind unsere Stammdaten. Namen, IDs und Geokoordinaten.
*Die Rolle:* Sie sind das Fundament. Ohne die Koordinaten wüssten wir nicht, wo das Wetter stattfindet.

**2. DB Timetables (Plan):**
Jeden Morgen um 01:00 Uhr laden wir den kompletten **Soll-Fahrplan** für die nächsten 24 Stunden.
*Die Rolle:* Das ist unsere Basislinie. Um eine Verspätung zu berechnen, muss man erst wissen, wann der Zug eigentlich kommen *sollte*.

**3. DB Timetables (Fchg - Full Changes):**
Alle 10 Minuten ziehen wir die kompletten Echtzeit-Daten.
*Die Rolle:* Das ist die "Wahrheit". Hier stehen die aktuellen Verspätungen und Gleiswechsel drin.

**4. DB Timetables (Rchg - Recent Changes):**
Parallel dazu laden wir die inkrementellen Updates.
*Die Rolle:* Das ist für die Effizienz. Statt immer alles neu zu verarbeiten, sehen wir hier schnell, was sich *gerade eben* geändert hat.

**5. Open-Meteo (History):**
Wir laden das Wetter der Vergangenheit.
*Die Rolle:* Das ist das Gedächtnis. Damit lernt die KI Zusammenhänge wie "Sturm = Verspätung".

**6. Open-Meteo (Forecast):**
Und schließlich die Wettervorhersage.
*Die Rolle:* Das ist der Blick in die Zukunft. Wenn für morgen Schnee gemeldet ist, weiß das System schon heute Bescheid.

---

## [08:30 - 09:30] Der Herzschlag (Orchestrierung)
*(Slide 7: Der Herzschlag)*

Daten zu haben ist das eine. Sie zur richtigen Zeit zu haben, ist das andere.
Wir nutzen **Apache Airflow**, um dem System einen Rhythmus zu geben.

Sehen Sie auf die Zeitleiste:
Der **10-Minuten-Takt** für Echtzeitdaten ist unser Puls.
*Warum nicht jede Sekunde?* Weil wir API-Limits respektieren müssen und Batch-Verarbeitung für Analysen stabiler ist als Streaming.
Der **Tages-Job** für den Plan läuft nachts, wenn das System ruhig ist.

Das Wichtigste hierbei ist die **Idempotenz**: Wenn ein Job um 14:00 Uhr fehlschlägt, können wir ihn um 14:10 Uhr einfach wiederholen. Das System heilt sich selbst, ohne Daten doppelt zu speichern.

---

## [09:30 - 11:30] Der Datenfluss (Der Prozess)
*(Slide 8: Der Data Flow)*

Wie wird aus diesen Rohdaten nun Information? Wir folgen einem klassischen **ELT-Prozess** (Extract, Load, Transform).

Schauen Sie auf den Ablauf:
1.  **Ingestion (Staging):** Zuerst holen wir die Daten rein. Wir speichern sie "as is" – also genau so, wie sie kommen. Das ist wichtig für die Beweissicherheit und Geschwindigkeit.
2.  **Historisierung (PSA):** Dann archivieren wir alles in der "Persistent Staging Area". Das ist unser goldenes Archiv. Selbst wenn wir später Fehler machen, die Originaldaten sind sicher.
3.  **Veredelung (DWH):** Erst im Data Warehouse wandeln wir das kryptische XML in lesbare Tabellen um. Hier entsteht der eigentliche Wert. Aus technischen Kürzeln werden verständliche Informationen.

---

## [11:30 - 13:00] Qualität & Sicherheit
*(Slide 9: Qualitätssicherung)*

Ein Informationssystem, dem man nicht vertraut, ist nutzlos. Deshalb ist **Qualitätssicherung** kein Luxus, sondern Pflicht.

Ein riesiges Problem waren **Duplikate**. Die APIs senden oft dieselben Daten mehrfach. Würden wir das einfach so in die KI füttern, würde sie völlig falsch lernen.
Ich habe deshalb Mechanismen auf Datenbank-Ebene implementiert, die Duplikate intelligent erkennen und entfernen.

Und was passiert, wenn das System ausfällt? Da ich nicht 24 Stunden vor dem Bildschirm sitze, habe ich ein **Monitoring** eingebaut. Wenn eine Daten-Pipeline bricht, sendet der Server automatisch eine E-Mail-Warnung. Das System meldet sich also proaktiv, wenn es Hilfe braucht.

---

## [13:00 - 14:30] Die Herausforderungen
*(Slide 10: Die Herausforderungen)*

Ich möchte ehrlich zu Ihnen sein. Der Weg hierher war steinig.

Die größte Hürde war nicht die KI, sondern das **Verständnis der Daten**.
Die Deutsche Bahn liefert ihre Daten in einem XML-Format, das extrem komplex und kaum dokumentiert ist. Sehen Sie sich diesen Code-Schnipsel an. Kürzel wie `pt`, `pp`, `l`... ohne Erklärung. Ich musste quasi "Reverse Engineering" betreiben, um diese Hieroglyphen zu entschlüsseln.

Dazu kommen inkonsistente Formate und fehlende Verknüpfungen.

Ein weiteres Problem war die **Performance**. Wenn Sie Millionen von Datensätzen in einem Dashboard anzeigen wollen, wird es langsam. Ich musste eine **serverseitige Filterung und Paginierung** implementieren, damit das Dashboard trotz "Big Data" flüssig und intuitiv bleibt. Wir laden also immer nur den relevanten Kontext (Echtzeit + 24h), statt die Datenbank zu überlasten.

Das System muss also extrem robust sein, um nicht bei jedem kleinen Datenfehler abzustürzen. Das hat mich 80% der Entwicklungszeit gekostet. Aber genau das unterscheidet ein akademisches Spielzeug von einem realen System.

---

## [14:30 - 16:00] Das Gehirn (Machine Learning)
*(Slide 11: Das Gehirn)*

Bevor ich Ihnen das System live zeige, müssen wir über sein Herzstück sprechen – oder besser gesagt: sein Gehirn.

Wir nutzen ein **LSTM-Netzwerk** (Long Short-Term Memory).
Vielleicht fragen Sie sich: *"Warum so kompliziert? Warum keine einfache Formel?"*

Stellen Sie sich vor, Sie lesen einen Satz. Um das letzte Wort zu verstehen, müssen Sie den Anfang des Satzes noch im Kopf haben.
Genau so funktionieren Züge. Eine Verspätung ist kein isolierter Punkt. Sie ist eine Geschichte. Ein Zug, der in Hamburg 5 Minuten Verspätung hat und in Bremen 10 Minuten, hat einen negativen **Trend**. Ein einfaches Modell sieht nur "Bremen". Das LSTM sieht die ganze Reise.

**Ich möchte Ihnen drei wichtige technische Entscheidungen erklären, die wir getroffen haben:**

1.  **Das Training (Sliding Window):**
    Wir trainieren das Modell **stündlich** komplett neu, basierend auf den Daten der letzten 24 Stunden.
    *Warum?* Das ist ein **Adaptive-Learning**-Ansatz. Das Wetter und die Betriebslage ändern sich schnell. Daten von vor einem Jahr sind für die Verspätung *heute* oft irrelevant. Durch dieses "Kurzzeitgedächtnis" reagiert das System extrem sensibel auf aktuelle Störungen (z.B. Sturm am Morgen).

2.  **Die Datenhaltung vs. Training:**
    Wir speichern zwar über **18 Millionen Ereignisse** in unserem Data Warehouse (für Analysen), aber das operative Modell bleibt schlank.
    *Der Vorteil:* Das Training dauert nur wenige Minuten statt Stunden. So können wir den stündlichen Takt überhaupt erst einhalten und garantieren, dass das Modell immer aktuell ist.

3.  **Die Architektur:**
    Wir nutzen **zwei Schichten mit je 64 Neuronen** und eine Sequenzlänge von 5 Stationen. Das ist der Sweet-Spot zwischen Genauigkeit und Rechenzeit.

Das ist der entscheidende Unterschied zwischen einer statischen Fahrplan-Berechnung und einer echten KI-Vorhersage, die aus der aktuellen Lage lernt.

---

## [16:00 - 17:00] Fazit & Abschluss
*(Slide 12: Fazit)*

**Was ist also das Fazit?**

Mein wichtigstes Learning aus diesem Projekt ist: **Architecture First.**
Man kann das beste KI-Modell der Welt haben. Wenn die Datenpipeline brüchig ist, wenn die Datenqualität schlecht ist, dann ist das Ergebnis wertlos. "Garbage In, Garbage Out".

Dieses System beweist, dass es möglich ist, mit begrenzten Mitteln eine professionelle Datenplattform aufzubauen, die echte Einblicke liefert. Wir haben die Frage "Kommt er noch?" vielleicht noch nicht final gelöst, aber wir haben das Werkzeug gebaut, um die Antwort zu finden.

---

## [17:00 - 20:00] Live Demo
*(Slide 13: Live Demo)*

Genug der Theorie. Ich möchte Ihnen beweisen, dass dieses System lebt und arbeitet.
Ich verbinde mich jetzt live mit meinem Server, auf dem das System läuft.

**(Aktion: Terminal öffnen)**

Zuerst möchte ich Ihnen zeigen, über welche Dimensionen wir hier sprechen. Wir sammeln seit Monaten Daten im 10-Minuten-Takt.

Schauen wir uns den Speicherverbrauch der Datenbank an:

*(Befehl eingeben)*
`docker exec -it train_postgres psql -U dw -d train_dw -c "SELECT pg_size_pretty(pg_database_size('train_dw')) AS db_size;"`

**(Ergebnis abwarten & kommentieren)**
Sehen Sie? Über **5 Gigabyte**. Das sind reine Textdaten! Das ist eine enorme Menge an Fahrplaninformationen.

Und jetzt schauen wir uns an, wie viele einzelne Ereignisse – also Ankünfte, Abfahrten, Änderungen – wir bereits verarbeitet haben:

*(Befehl eingeben)*
`docker exec -it train_postgres psql -U dw -d train_dw -c "SELECT count(*) FROM dwh.timetables_fchg_events;"`

**(Ergebnis abwarten & kommentieren)**
Über **18 Millionen** Datensätze. Jeder einzelne davon ist ein Puzzleteil, das unser System nutzt, um zu lernen.

**(Aktion: Kurz das Dashboard zeigen)**
Hier sehen Sie das Dashboard, das auf diesen Daten operiert.

Lassen Sie uns kurz durch die wichtigsten Funktionen gehen:

**1. Die Übersicht:**
Ganz oben sehen Sie den **Systemstatus**. Es ist entscheidend zu wissen, ob die Daten aktuell sind. Hier sehen Sie "Letzte Aktualisierung" in Echtzeit.

**2. Die Datenliste & Performance:**
Darunter sehen Sie die Liste der nächsten Züge.
Erinnern Sie sich an das Performance-Problem? Hier greift die **Paginierung**. Wir laden nicht alle Millionen Datensätze, sondern blättern intelligent durch die Seiten. Das macht die Bedienung flüssig.

**3. Risiko-Filterung:**
Jetzt wird es interessant. Wenn ich auf den Filter **"Hohes Risiko"** klicke...
*(Klick auf Button "Hohes Risiko")*
...filtert das System sofort alle Züge mit mehr als 5 Minuten prognostizierter Verspätung. Das ist der Mehrwert für den Nutzer: "Welche Züge muss ich meiden?"

**4. Detail-Ansicht:**
Wenn ich nun einen dieser Züge anklicke...
*(Klick auf eine Zeile)*
...öffnet sich die Detail-Ansicht. Hier sehen wir die **gesamte Route** visualisiert. Wir sehen nicht nur, wo der Zug ist, sondern wie sich die Verspätung entlang der Strecke entwickelt hat.

**(Optional: Charts zeigen)**
Und unten sehen Sie die **Risiko-Analyse**. Die Heatmap zeigt uns auf einen Blick, zu welcher Uhrzeit an welchem Bahnhof die meisten Probleme auftreten.

Das ist "Actionable Intelligence" – Daten, mit denen man Entscheidungen treffen kann.

Vielen Dank für Ihre Aufmerksamkeit.
