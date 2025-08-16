# Stammdaten-Tool

Dieses kleine Projekt demonstriert **Stammdatenpflege, Validierung und Speicherung**

## Funktionen
- CSV-Datei (`data/sample_raw.csv`) einlesen
- Dubletten und fehlerhafte Zeilen erkennen
- Bereinigte Daten nach **SQLite** speichern (`stammdaten.db`)
- Report erzeugen (`docs/report.md`)
- Getrennte Ausgaben: `data/clean.csv` (gültig), `data/rejects.csv` (ungültig)
