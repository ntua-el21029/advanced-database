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
| **JSON (Explicit Schema)** | ~18.1 sec |  0 for Inference | 40 for WholeStageCodegen |
| **Parquet** | ~6.6 sec | 0 for inference | 44 for computation |

*Σημείωση: Οι χρόνοι αποτελούν τον διάμεσο 3 διαδοχικών εκτελέσεων με χρήση της βιβλιοθήκης `time` της Python.*

---

# 3. Ανάλυση Πλάνου Εκτέλεσης (Physical Plan)

Η μελέτη του Physical Plan μέσω της συνάρτησης `explain()` επιβεβαιώνει τη διαφορά απόδοσης των τριών μεθόδων, εστιάζοντας στον μηχανισμό ανάγνωσης (FileScan):

* **ReadSchema (Column Pruning):** Στην εκτέλεση του Parquet, το `ReadSchema` περιορίζεται αυστηρά στις στήλες `date` και `stars`. Το Spark εκμεταλλεύεται την columnar δομή και φορτώνει από τον δίσκο αποκλειστικά αυτά τα δεδομένα. Αντίθετα, στα JSON (ακόμα και με ρητό schema), ο row-based parser αναγκάζεται να σαρώσει ολόκληρη τη δομή κάθε γραμμής, μεταφέροντας στη μνήμη περιττό όγκο δεδομένων (όπως το μεγάλο πεδίο `text`).
* **PushedFilters (Predicate Pushdown):** Ο πίνακας `PushedFilters` εμφανίζεται κενός (`[]`) σε όλες τις εκτελέσεις. Αυτό είναι το αναμενόμενο, καθώς το ζητούμενο ερώτημα υπολογίζει τον μέσο όρο στο σύνολο του dataset και δεν περιέχει ρήτρες φιλτραρίσματος (π.χ. `WHERE` / `filter()`), συνεπώς δεν υπάρχουν συνθήκες προς επιβολή στο επίπεδο του δίσκου.
* **PartitionFilters:** Παρομοίως, τα `PartitionFilters` είναι κενά (`[]`), διότι τα δεδομένα προέρχονται από ενιαία αρχεία και δεν έχει εφαρμοστεί κατάτμηση σε επίπεδο φακέλων (π.χ. `partitionBy("year")`).





---
# Query 1

 Προσθέτοντας την γραμμή result.explain() προκύπτει ότι:

* ``` bash
  ReadSchema: struct<stars:float,useful:int,text:string>
  ```
  Το Spark λόγω της columnar δομής του Parquet file, δεν διαβάζει ολόκληρο το αρχείο. Φορτώνει στη μνήμη μόνο τις 3 στήλες (stars, useful, text), δηλαδή ακριβώς αυτές που χρειάζονται για τα φίλτρα και τους υπολογισμούς του query, σε αντίθεση με json file όπους θα έπρεπε να διαβάσει κάθε πεδίο του.

* 
  ``` bash
  PushedFilters: [IsNotNull(useful), GreaterThanOrEqual(useful,1)]
  ```
  Το φίλτρο useful >= 1 και ο έλεγχος για NULL στη στήλη useful χρησιμοποιούνται ως PushedFilters. Δηλαδή το Spark μεταφέρει αυτά τα φίλτρα στον SSD. Το Parquet διαβάζει τα metadata του και απορρίπτει τα blocks του αρχείου που έχουν αποκλειστικά useful < 1 χωρίς να τα στείλει στη μνήμη RAM.

* ``` bash
  PartitionFilters: [isnotnull(year#9), (year#9 >= 2015), (year#9 <= 2021)]
  ```
  Το Spark αναγνωρίζοντας τη δομή των καταλόγων του συστήματος αρχείων (όπου κάθε έτος αποτελεί ξεχωριστό υποφάκελο), αξιοποιεί τον μηχανισμό Partition Pruning. Μέσω της αξιολόγησης των metadata, ο Optimizer αποκλείει εξ αρχής καταλόγους που δεν ικανοποιούν τη συγκεκριμένη χρονική συνθήκη δηλαδή τα έτη πριν το 2015 και μέτα το 2021. Η προσέγγιση αυτή ελαχιστοποιεί δραστικά τον όγκο των δεδομένων που ανακτώνται από τον δίσκο (Disk I/O) και την δέσμευση πόρων της RAM για εγγραφές που θα απορρίπτονταν μετέπειτα.

* ``` bash
   Filter (isnotnull(useful#4) AND (useful#4 >= 1))
   ```
    Αναπαριστά τον έλεγχο που πραγματοποιείται in-memory αφού τα δεδομένα έχουν φορτωθεί στη μνήμη RAM. Επειδή μετά το στάδιο της ανάγνωσης καθώς ο έλεγχος year δεν γίνεται καν αφού η συγκεκριμένη συνθήκη έχει ήδη ικανοποιηθεί από το Partition Pruning κατά το στάδιο της ανάγνωσης. Άρα το CPU αφορά αποκλειστικά στη σάρωση εγγραφής προς εγγραφή (row-level filtering) για την αξιολόγηση της στήλης useful και βελιώνει την απόδοση της επεξεργασίας του φιλτραρισμένου συνόλου.

* **Σύγκριση χρόνων με cache() και χωρίς** :
    Παρά το αρχικό κόστος φόρτωσης των δεδομένων στη RAM, η χρήση της μεθόδου .cache() μείωσε τον συνολικό χρόνο εκτέλεσης από 19.55s σε 12.35s, καθώς μηδένισε το Disk I/O για τα δύο επακόλουθα queries. Ουσιαστικά μετά το query a η RAM δεν αδειάζει άρα το b τρέχει πιο γρήγορα.

* **Σύγκριση Μεγέθους (Cache vs Parquet)**:
Από την καρτέλα Storage του Spark UI, διαπιστώθηκε ότι το cached σύνολο δεδομένων καταλαμβάνει 1642.4 MiB (1.64 GB) στη RAM. Το συνολικό μέγεθος των αντίστοιχων αρχείων Parquet (για τα έτη 2015-2021) στον SSD είναι 2.1 GB. Το μέγεθος στη μνήμη είναι μικρότερο από το μέγεθος των συμπιεσμένων αρχείων στον δίσκο εξηγείται από τη λειτουργία του φιλτραρίσματος. Το Spark έκανε cache το DataFrame αφού πρώτα εφάρμοσε το φίλτρο useful >= 1. Άρα ένα μεγάλο πλήθος εγγραφών απορρίφθηκε κατά την ανάγνωση και δεν φορτώθηκε ποτέ στη RAM

* **Cache whole Parquet reviews:**
   ```bash
   Caused by: java.lang.OutOfMemoryError: Java heap space
   at java.base/java.nio.HeapByteBuffer.<init>
    ```

Αυτό δείχνει ότι την ώρα που το Spark διάβαζε τα Parquet (και ειδικά τα τεράστια Strings του text) και προσπαθούσε να τα μετατρέψει σε αντικείμενα Java (HeapByteBuffer) για να τα βάλει στη RAM, η μνήμη των 2GB του executor γέμισε και η Java κατέρρευσε.



---
# Query 2

* **Σύγκριση Physical Plan:**
  Τα Physical Plans είναι ίδια. Αξιοποιώντας το Lazy Evaluation, το Spark δεν εκτέλεσε τον κώδικα γραμμή προς γραμμή. Αυτό που έκανε ήτανότι περίμενε μέχρι το .show(), κατάλαβε τη λογική και παρήγαγε το ίδιο δέντρο 35 βημάτων και για τα δύο ερωτήματα επιλέγοντας τους ίδιους αλγορίθμους ( BroadcastHashJoin, HashAggregate, WindowGroupLimit). Επίσης προκύπτει ο ίδιος αριθμός από Exchange (4 κανονικά shuffles και 2 broadcast exchanges) και τα ίδια Scan parquet (με column pruning και filter pushdown) η συμπεριφορά τους σε επίπεδο πόρων (RAM, CPU, Δίκτυο) είναι ταυτόσημη.

* **Σύγκριση χρόνων εκτέλεσης:** 
Από 5 επαναλήψεις προέκυψε ότι ο μέσος χρόνος εκτέλεσης με το DataFrame API είναι ~83.16 sec, ενώ με το SQL API είναι ~64.83 sec άρα είναι πρακτικά ίδιοι. Το μεγάλο variance (εως 30sec) εξηγούνται από τον έντονο ανταγωνισμό πόρων (Resource Contention) στο WSL: η ταυτόχρονη λειτουργία πολλαπλών JVMs (Master, Worker, Driver και 4 Executors με δέσμευση 8 GB RAM), οι παύσεις του Garbage Collector (GC pauses), η σειριακοποίηση δεδομένων μέσω δικτύου (IPC/loopback network στα 4 Exchange) και τα bottlenecks στο Disk I/O κατά την ταυτόχρονη ανάγνωση των Parquet αρχείων προκαλούν έντονες αυξομειώσεις στον χρόνο απόκρισης του συστήματος.

* Ο Catalyst optimizer επέλεξε τη στρατηγική Broadcast       Hash Join δηλαδή βελτιστοποίηση σε επίπεδο δικτύου (Broadcast) και σε επίπεδο επεξεργασίας (Hash). Για το Broadcast: επειδή ο πίνακας των business είναι πολύ μικρός το Spark τον μεταδίδει αυτούσιο (broadcast) στη μνήμη κάθε executor. Αυτό είναι σημαντικό για την απόδοση, καθώς αποφεύγεται πλήρως το Network Shuffle (ανταλλαγή δεδομένων μέσω δικτύου) κατά το join. Αν δεν ήταν broadcast κάθε executor θα είχε ένα υποσύνολο τον business για να κάνει join με subset των reviews άρα μετά θα έπρεπε να κάνει shuffle στον επόμενο executor. Το Hash κομμάτι εξασφαλίζει ότι ο μικρός πίνακας θα μετατραπεί σε ένα Hash Table (dictionary O(1)) στη RAM του κάθε executor. Άρα, καθώς κάθε κομμάτι (partition) του πίνακα των reviews διαβάζεται σταδιακά στη ram κάθε executοr, η σύγκριση γίνεται ακαριαία διατηρώντας την κατανάλωση μνήμης σταθερή και χαμηλή.

* **Exchange (6) — hashpartitioning(state, 200):** Οφείλεται στο GROUP BY state πάνω στον πίνακα business. Aφού κάθε executor υπολογίσει τοπικά τα μερικά αθροίσματα (partial_count στο HashAggregate (5)), το Exchange (6) ανακατανέμει τα δεδομένα με βάση το state, ώστε οι εγγραφές της ίδιας πολιτείας να βρεθούν στον ίδιο executor για το τελικό άθροισμα (HashAggregate (7)).

*  **Exchange (22) — hashpartitioning(state, category, 200):** Οφείλεται στο (GROUP BY state, category) μετά το explode των κατηγοριών και την ένωση με τον πίνακα review. Μεταφέρει μέσω δικτύου τα τοπικά προ-αθροισμένα αποτελέσματα (partial_count, partial_avg, partial_sum από το HashAggregate (21)), ώστε όλα τα δεδομένα που αφορούν το ίδιο ζευγάρι (state, category) να συγκεντρωθούν στον ίδιο κόμβο και να υπολογιστούν οι τελικές μετρικές (total_reviews, avg_stars, percent_5_stars) στο HashAggregate (23).

* **Exchange (27) — hashpartitioning(state, 200):** Οφείλεται στο Window Function: ROW_NUMBER() OVER (PARTITION BY state ORDER BY avg_stars DESC, total_reviews DESC) για την εύρεση των Top-3 κατηγοριών ανά πολιτεία.

* **Exchange (33) — rangepartitioning(state ASC, avg_stars DESC, 200):**  Στην τελική ταξινόμηση (ORDER BY state ASC, avg_stars DESC) του αποτελέσματος (Sort (34)). Χρησιμοποιεί rangepartitioning ώστε να κατανείμει τις τελικές εγγραφές σε διατεταγμένα εύρη τιμών μεταξύ των partitions πριν την τελική εμφάνιση.

---
# Query 3