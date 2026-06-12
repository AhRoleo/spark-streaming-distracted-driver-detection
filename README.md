# spark-file-streaming

Simulation de **Spark Structured Streaming** avec Scala et SBT.  
Le projet produit des fichiers par lots (batches) depuis un dossier source vers un dossier de destination, simulant un flux de données en streaming à intervalles réguliers.

> **Contexte pédagogique** : Cours Spark Streaming — ESGI 4ème année

---

## Architecture multi-module

Le projet est organisé en **3 modules SBT indépendants** :

```
spark-file-streaming/
├── build.sbt                              ← build multi-module SBT
├── project/
│   ├── build.properties                   ← version SBT
│   └── plugins.sbt                        ← sbt-assembly
│
├── modules/
│   │
│   ├── common/                            ← utilitaires partagés
│   │   └── src/main/
│   │       ├── resources/
│   │       │   └── logback.xml            ← configuration des logs (niveau WARN)
│   │       └── scala/common/spark/
│   │           └── SparkSessionFactory.scala  ← loan pattern (withSession)
│   │
│   ├── producer/                          ← programme producteur
│   │   └── src/main/
│   │       ├── resources/
│   │       │   └── application.properties ← paramètres du producer
│   │       └── scala/producer/
│   │           ├── FileProducer.scala     ← point d'entrée (main)
│   │           ├── config/
│   │           │   └── ProducerConfig.scala   ← chargeur de config typé
│   │           └── helpers/
│   │               └── FileTransfer.scala     ← wrapper Hadoop FileUtil
│   │
│   └── consumer/                          ← programme consommateur (squelette)
│       └── src/main/
│           ├── resources/
│           │   └── application.properties ← paramètres du consumer
│           └── scala/consumer/
│               ├── StructuredStreamingConsumer.scala  ← point d'entrée (à implémenter)
│               └── config/
│                   └── ConsumerConfig.scala   ← chargeur de config typé
│
├── scripts/
│   └── clean.sh                           ← réinitialise le sandbox de streaming
│
└── data/
    ├── source/                            ← déposer les fichiers à produire ici
    └── destination/                       ← contrat entre producer et consumer
```

---

## Graphe de dépendances des modules

```
producer ──┐
           ├──► common
consumer ──┘
```

- **`common`** n'a aucune dépendance interne
- **`producer`** et **`consumer`** dépendent tous les deux de `common`

---

## Prérequis

| Outil | Version recommandée |
|---|---|
| Java | 17 (OpenJDK) |
| Scala | 2.12.17 |
| SBT | 1.9+ |
| Apache Spark | 3.3.2 |

---

## Configuration

### Producer — `modules/producer/src/main/resources/application.properties`

| Propriété | Description | Valeur par défaut |
|---|---|---|
| `producer.input-dir` | Dossier source des fichiers à envoyer | `data/source` |
| `producer.output-dir` | Dossier de destination (lu par le consumer) | `data/destination` |
| `producer.batch-size` | Nombre de fichiers copiés par lot | `1` |
| `producer.interval-time` | Délai entre deux lots **en secondes** | `5` |

### Consumer — `modules/consumer/src/main/resources/application.properties`

| Propriété | Description | Valeur par défaut |
|---|---|---|
| `consumer.input-dir` | Dossier surveillé en streaming | `data/destination` |
| `consumer.checkpoint-dir` | Dossier de checkpoint Spark (fault tolerance) | `data/checkpoint` |

---

## Lancement

> **Note** : Toutes les commandes sont à exécuter depuis la racine du projet.

### Compiler tous les modules
```bash
sbt compile
```

### Lancer le producer
```bash
sbt "producer/run"
```

### Lancer le consumer *(squelette — à implémenter)*
```bash
sbt "consumer/run"
```

### Réinitialiser le sandbox *(vider destination + checkpoint)*
```bash
bash scripts/clean.sh
```

### Compiler un seul module
```bash
sbt "common/compile"
sbt "producer/compile"
sbt "consumer/compile"
```

---

## Fonctionnement du producer

1. Chargement de la configuration depuis `application.properties`
2. Initialisation de la `SparkSession` via `SparkSessionFactory.withSession`
3. Listing des fichiers présents dans `data/source/`
4. Découpage en lots de taille `batch-size`
5. Pour chaque lot :
   - Copie de chaque fichier vers `data/destination/` via `FileTransfer.copyFile`
   - Pause de `interval-time` secondes (simulation de l'intervalle de streaming)
6. Arrêt automatique de Spark à la fin du traitement (loan pattern)

```
=== FileProducer (Simulation de streaming) ===
  input-dir     : data/source
  output-dir    : data/destination
  batch-size    : 1 fichier(s) par batch
  interval-time : 5s
=============================================

Envoi depuis 'data/source' vers 'data/destination' par lot de 1 fichier(s), intervalle : 5s
  [OK] Fichier copié : fichier1.txt
  Batch terminé. Attente de 5s avant le prochain lot...
  [OK] Fichier copié : fichier2.txt
  Batch terminé. Attente de 5s avant le prochain lot...

[FIN] Tous les lots ont été traités.
```

---

## Description des fichiers clés

### `common/spark/SparkSessionFactory.scala` — Loan pattern
Centralise la création et l'arrêt de la `SparkSession`.  
Garantit que Spark est **toujours arrêté proprement**, même en cas d'exception.

```scala
SparkSessionFactory.withSession("MonApp") { spark =>
  // Spark est disponible ici
  // Il sera automatiquement arrêté à la sortie du bloc
}
```

### `producer/config/ProducerConfig.scala` — Config typée
Charge `application.properties` et expose chaque paramètre sous forme d'un **case class Scala typé** (pas de `.getProperty("clé")` dispersés dans le code).

### `producer/helpers/FileTransfer.scala` — Wrapper Hadoop
Encapsule `FileUtil.copy` avec des paramètres explicites :
- `deleteSource = false` → le fichier source est **conservé**
- `overwrite = true` → écrase le fichier si déjà présent à destination

### `consumer/StructuredStreamingConsumer.scala` — Squelette
Point d'entrée du consumer. La logique Structured Streaming est à implémenter dans la méthode `run`.  
Des exemples commentés (readStream, writeStream, awaitTermination) sont fournis comme guide.

---

## Stack technique

| Composant | Technologie |
|---|---|
| Langage | Scala 2.12 |
| Framework distribué | Apache Spark 3.3.2 (Core + SQL) |
| Build tool | SBT 1.9 (multi-module) |
| Système de fichiers | Hadoop FileSystem API |
| Logging | Logback (via logback.xml partagé) |
| Packaging | sbt-assembly |

---

## Remarques Java 17

Spark 3.3.x n'est pas nativement compatible avec Java 17.  
Le `build.sbt` configure automatiquement les options `--add-opens` nécessaires pour contourner les restrictions du module système Java 17 :

```scala
javaOptions ++= Seq(
  "--add-opens", "java.base/java.lang=ALL-UNNAMED",
  // ...
)
```

Ces options sont définies une seule fois dans `commonSettings` et héritées par tous les modules.
