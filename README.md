# Oracle → MSSQL Data Migration Console

A modern, GUI-based data migration tool for moving data from **Oracle** to **Microsoft SQL Server** with high reliability, clean UI, and full control over SQL queries.

This tool includes a SQL editor, automatic datatype handling, real-time migration logs, Light/Dark themes and full EXE build support using PyInstaller.

---

##  Key Features

### 🔹 Modern UI (IDE-Style)
- Light/Grey & Dark themes  
- SVG icons with automatic theme switching  
- Responsive layout with professional styling

### 🔹 Advanced SQL Editor
- Syntax highlighting  
- Autocomplete (Ctrl+Space)  
- SQL snippets (Tab)  
- Error underline for invalid SQL  
- Auto-format (Ctrl+Shift+F)  
- Line numbers & minimap

### 🔹 Powerful Migration Engine
- Oracle Thin mode support  
- In-memory query execution (no temp file required)  
- Fixed MS SQL datatype formatting (datetime, numeric, varchar)  
- Faster, safer, more stable inserts  
- Auto CSV export using toggle switch  

### 🔹 Real-Time Logs
- Color-coded logs  
- “Waiting for migration to start…” initial state  
- Clear logs button  
- Log file saved to `oracle_to_mssql.log`

### 🔹 EXE-Ready Build
- PyInstaller-compatible  
- Bundles all required Qt plugins  
- Works fully offline  
- Includes theme-specific icon packs

---

##  Tech Stack

- **Python 3.10+**
- **PyQt6** UI Framework  
- **OracleDB (Thin Mode)**  
- **MS SQL (pyodbc)**  
- **pandas, sqlparse, pillow**  
- **cryptography 38.0.4 (EXE-safe)**  

---

##  License
Internal-use only (UIIC).

---

##  Developer and Maintainer
Sanju Sarkar  
Administrative Officer, Actuarial Department, Head Office, United India Insurance Company Limited

---

