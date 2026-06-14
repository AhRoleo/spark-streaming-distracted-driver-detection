# Guide des commandes WSL — spark-file-streaming

> Toutes les commandes sont à exécuter dans **WSL** (Ubuntu).  
> Le projet se trouve à : `/mnt/c/Users/joane/Desktop/ESGI4/Spark Streaming/mini_test`

---

## 1. Se placer dans le projet

La **première chose à faire** à chaque session WSL est de naviguer vers le projet :

```bash
cd '/mnt/c/Users/joane/Desktop/ESGI4/Spark Streaming/mini_test'
```

> ⚠️ Les guillemets sont obligatoires à cause de l'espace dans `Spark Streaming`.

---

## 2. Compiler

### Compiler tous les modules (common + producer + consumer)
```bash
sbt compile
```

### Compiler un seul module
```bash
sbt "common/compile"
sbt "producer/compile"
sbt "consumer/compile"
```

### Recompiler depuis zéro (après un gros changement)
```bash
sbt clean compile
```

> 💡 `sbt compile` est **incrémental** : il ne recompile que les fichiers modifiés.  
> À utiliser après chaque modification de code avant de relancer.

---

## 3. Lancer le producer

```bash
sbt "producer/run"
```

Le producer va :
1. Lire les fichiers dans `data/source/`
2. Les copier vers `data/destination/` un par un (batch-size = 1)
3. Attendre 5 secondes entre chaque fichier (interval-time = 5)

### Modifier la config avant de lancer
Editer `modules/producer/src/main/resources/application.properties` :
```properties
producer.batch-size    = 3    # Copier 3 fichiers par lot
producer.interval-time = 2    # Attendre 2 secondes entre chaque lot
```
Puis relancer directement (pas besoin de recompiler pour un `.properties`) :
```bash
sbt "producer/run"
```

---

## 4. Lancer le consumer *(squelette à implémenter)*

```bash
sbt "consumer/run"
```

---

## 5. Réinitialiser le sandbox

Vide `data/destination/` et `data/checkpoint/` pour repartir à zéro :

```bash
bash scripts/clean.sh
```

Ou manuellement :
```bash
rm -rf data/destination/*
rm -rf data/checkpoint/*
```

---

## 6. Workflow type : modifier → compiler → tester

```bash
# 1. Se placer dans le projet
cd '/mnt/c/Users/joane/Desktop/ESGI4/Spark Streaming/mini_test'

# 2. (Optionnel) Nettoyer la destination pour repartir de zéro
bash scripts/clean.sh

# 3. Compiler après modification du code Scala
sbt compile

# 4. Lancer le producer
sbt "producer/run"
```

---

## 7. Mode shell interactif SBT (plus rapide)

Au lieu de relancer `sbt` à chaque commande (ce qui prend ~5s de démarrage à chaque fois),
tu peux **ouvrir le shell SBT une seule fois** et taper les commandes dedans :

```bash
sbt
```

Puis dans le shell SBT :
```
sbt:spark-file-streaming> compile
sbt:spark-file-streaming> producer/run
sbt:spark-file-streaming> clean
sbt:spark-file-streaming> exit
```

> 💡 En mode shell, le rechargement est quasi-instantané car la JVM reste en vie.

---

## 8. Commande complète en une ligne (depuis PowerShell Windows)

Si tu veux lancer depuis **PowerShell** (sans ouvrir WSL manuellement) :

```powershell
# Compiler
wsl bash -c "cd '/mnt/c/Users/joane/Desktop/ESGI4/Spark Streaming/mini_test' && sbt compile"

# Lancer le producer
wsl bash -c "cd '/mnt/c/Users/joane/Desktop/ESGI4/Spark Streaming/mini_test' && sbt 'producer/run'"

# Nettoyer + lancer le producer
wsl bash -c "cd '/mnt/c/Users/joane/Desktop/ESGI4/Spark Streaming/mini_test' && bash scripts/clean.sh && sbt 'producer/run'"
```

---

## 9. Résumé des commandes

| Action | Commande |
|---|---|
| Aller dans le projet | `cd '/mnt/c/Users/joane/Desktop/ESGI4/Spark Streaming/mini_test'` (WSL) |
| Compiler tout | `sbt compile` (WSL) |
| Compiler proprement | `sbt clean compile` (WSL) |
| Lancer le producer | `sbt "producer/run"` (WSL) |
| Lancer le consumer | `sbt "consumer/run"` (WSL) |
| Lancer le dashboard | `python -m streamlit run dashboard/app.py` (WSL / Windows) |
| Nettoyer destination | `bash scripts/clean.sh` (WSL) |
| Shell interactif | `sbt` (WSL) |
| Compiler un module | `sbt "producer/compile"` (WSL) |
