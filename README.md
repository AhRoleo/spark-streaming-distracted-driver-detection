# spark-streaming-distracted-driver-detection — branche `ansame-stream`

---

## Architecture globale

Voici le schéma d'architecture du projet que j'ai réalisé pour bien visualiser les différents composants et leurs interactions :

![Architecture du projet](archi.jpg)

On voit clairement les 4 microservices qui communiquent entre eux :
- le **Producer** (Spark Core) qui lit les images et les envoie vers `/dest/`
- le **ML Service** (Python) qui héberge le modèle et le distribue
- le **Consumer** (Spark Structured Streaming) qui récupère le modèle, analyse les images et écrit les résultats
- le **Dashboard** (Streamlit) qui lit les résultats et les affiche en temps réel

---

## Ma contribution — Microservice ML Python

Dans la version de base du projet (branche `darky-stream`), le consumer Spark allait chercher le modèle ONNX **directement sur le disque dur** en local. C'est fonctionnel mais ça ne correspond pas vraiment à une architecture microservices réelle.

J'ai donc créé un **microservice ML dédié** en Python avec FastAPI pour que le modèle soit hébergé de façon indépendante et accessible via HTTP, comme dans un vrai projet en production.

### Ce que j'ai ajouté

**Nouveau fichier `ml_service/app.py`**

Un serveur FastAPI avec 4 routes :

| Route | Ce que ça fait |
|---|---|
| `GET /health` | Vérifie que le service tourne bien |
| `GET /model` | Envoie le fichier ONNX au consumer qui le demande |
| `GET /model/info` | Retourne les infos du modèle (classes, dimensions) |
| `POST /predict` | Accepte une image et retourne la prédiction |

**Modifications dans le Consumer Scala**

- Ajout d'une fonction `fetchModelFromService()` dans `StructuredStreamingConsumer.scala` : au démarrage, le consumer appelle `GET /model` pour télécharger le modèle depuis le service au lieu de le lire depuis le disque
- Ajout du champ `modelUrl` dans `ConsumerConfig.scala`
- Ajout de `consumer.model-url = http://localhost:8000/model` dans `application.properties`

Si le service ML n'est pas disponible, le consumer bascule automatiquement sur le fichier local (fallback) pour ne pas planter.

### Pourquoi ce changement ?

Dans un vrai système embarqué avec plusieurs voitures, chaque voiture aurait son propre consumer. Avec l'ancienne version, il faudrait copier manuellement le fichier ONNX sur chaque machine. Avec le microservice, toutes les instances du consumer vont chercher le modèle au même endroit. Si on met à jour le modèle, tout le monde récupère la nouvelle version automatiquement au prochain démarrage.

### Lancer le ML Service

```bash
# Installer les dépendances
pip install -r ml_service/requirements.txt

# Démarrer le service (port 8000)
uvicorn ml_service.app:app --host 0.0.0.0 --port 8000
```

Ensuite lancer le consumer normalement, il va automatiquement chercher le modèle sur `http://localhost:8000/model`.
