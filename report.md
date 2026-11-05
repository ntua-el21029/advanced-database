# 1. Εγκατάσταση Cluster & Χαρακτηριστικά Συστήματος

## Χαρακτηριστικά Υλικού (Hardware) & Λειτουργικού Συστήματος
Οι μετρήσεις και η εκτέλεση του Spark πραγματοποιήθηκαν σε τοπικό περιβάλλον με τα εξής χαρακτηριστικά:
* **Λειτουργικό Σύστημα:** Windows 11 Version 25H2 με υποσύστημα WSL2 ( Ubuntu 22.04.5 LTS)
* **CPU:** AMD Ryzen 5 5600H with Radeon Graphics
* **Λογικοί Πυρήνες (Logical Cores):** 12
* **Μνήμη RAM:** 16 GB
* **Αποθηκευτικός Χώρος (Δίσκος):** SK Hynix HFS512GDE9X084N (512GB NVMe SSD)

## Αρχιτεκτονική Standalone Cluster
Το Apache Spark (έκδοση 4.2.0) ρυθμίστηκε σε λειτουργία **Standalone Cluster** εντός του περιβάλλοντος WSL. Η αρχιτεκτονική περιλαμβάνει:
* Έναν **Master Node** (εκτελείται στο `localhost:7077`), ο οποίος διαχειρίζεται τους πόρους και το Web UI του βρίσκεται στο port `localhost:8080`
* Έναν **Worker Node**, ο οποίος αναλαμβάνει την εκτέλεση των tasks, βρίσκεται σε random port και το Web UI του στο `localhost:8081`
* Τον **History Server** (εκτελείται στο `localhost:18080`), ο οποίος ρυθμίστηκε για την καταγραφή των συμβάντων (event logs) και την ανάλυση των σταδίων εκτέλεσης (stages/tasks) μετά την ολοκλήρωση των εφαρμογών.

**1. Εκκίνηση Master Node:**
```bash
./sbin/start-master.sh --host localhost --port 7077 --webui-port 8080
```

**2. Εκκίνηση Worker Node:**
```bash
./sbin/start-worker.sh spark://localhost:7077 --webui-port 8081
```

**3. Εκκίνηση History Server:**
```bash
./sbin/start-history-server.sh
```



---

# 2. Αξιολόγηση Απόδοσης (Benchmark) - JSON vs Parquet

Στον παρακάτω πίνακα παρουσιάζονται οι χρόνοι εκτέλεσης (wall-clock time) και ο αριθμός των tasks για τις τρεις διαφορετικές μεθόδους ανάγνωσης του dataset. 

| Μέθοδος Ανάγνωσης | Χρόνος Εκτέλεσης (Median) | Tasks (Inference) | Tasks (Computation) |
| :--- | :--- | :--- | :--- |
| **JSON (Schema Inference)** | ~29.57 sec | 40 (Job 0: Inference) | 40 (Job 1: Execution) |
| **JSON (Explicit Schema)** | ~18.1 sec | [ 0 for Inference] | [40 for WholeStageCodegen] |
| **Parquet** | ~13.07 sec | [Αριθμός από Stage 0] | [Αριθμός από Stage 1] |

*Σημείωση: Οι χρόνοι αποτελούν τον διάμεσο 3 διαδοχικών εκτελέσεων με χρήση της βιβλιοθήκης `time` της Python.*

---

# 3. Ανάλυση Πλάνου Εκτέλεσης (Physical Plan)

Η μελέτη του Physical Plan μέσω της συνάρτησης `explain()` επιβεβαιώνει τη διαφορά απόδοσης των τριών μεθόδων, εστιάζοντας στον μηχανισμό ανάγνωσης (FileScan):

* **ReadSchema (Column Pruning):** Στην εκτέλεση του Parquet, το `ReadSchema` περιορίζεται αυστηρά στις στήλες `date` και `stars`. Το Spark εκμεταλλεύεται την columnar δομή και φορτώνει από τον δίσκο αποκλειστικά αυτά τα δεδομένα. Αντίθετα, στα JSON (ακόμα και με ρητό schema), ο row-based parser αναγκάζεται να σαρώσει ολόκληρη τη δομή κάθε γραμμής, μεταφέροντας στη μνήμη περιττό όγκο δεδομένων (όπως το μεγάλο πεδίο `text`).
* **PushedFilters (Predicate Pushdown):** Ο πίνακας `PushedFilters` εμφανίζεται κενός (`[]`) σε όλες τις εκτελέσεις. Αυτό είναι το αναμενόμενο, καθώς το ζητούμενο ερώτημα υπολογίζει τον μέσο όρο στο σύνολο του dataset και δεν περιέχει ρήτρες φιλτραρίσματος (π.χ. `WHERE` / `filter()`), συνεπώς δεν υπάρχουν συνθήκες προς επιβολή στο επίπεδο του δίσκου.
* **PartitionFilters:** Παρομοίως, τα `PartitionFilters` είναι κενά (`[]`), διότι τα δεδομένα προέρχονται από ενιαία αρχεία και δεν έχει εφαρμοστεί κατάτμηση σε επίπεδο φακέλων (π.χ. `partitionBy("year")`).

