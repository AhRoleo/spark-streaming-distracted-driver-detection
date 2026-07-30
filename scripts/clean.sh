#!/bin/bash
# =============================================================================
# clean.sh — Réinitialise le sandbox de streaming
#
# Ce script vide le dossier data/destination pour pouvoir relancer
# le producer depuis le début sans fichiers résiduels.
# Usage : bash scripts/clean.sh (depuis la racine du projet)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DESTINATION="$PROJECT_ROOT/data/destination"
CHECKPOINT="$PROJECT_ROOT/data/checkpoint"
OUTPUT="$PROJECT_ROOT/data/output"
SOURCE="$PROJECT_ROOT/data/source"
TEST_IMAGES="$PROJECT_ROOT/data/test-images"

echo "=== Nettoyage du sandbox de streaming ==="

if [ -d "$DESTINATION" ]; then
  rm -rf "$DESTINATION"
  echo "[OK] data/destination supprimé."
else
  echo "[INFO] data/destination n'existe pas."
fi

if [ -d "$CHECKPOINT" ]; then
  rm -rf "$CHECKPOINT"
  echo "[OK] data/checkpoint supprimé."
else
  echo "[INFO] data/checkpoint n'existe pas."
fi

if [ -d "$OUTPUT" ]; then
  rm -rf "$OUTPUT"
  echo "[OK] data/output supprimé."
else
  echo "[INFO] data/output n'existe pas."
fi

if [ -d "$SOURCE" ]; then
  # Supprimer les anciens fichiers texte (.txt)
  rm -f "$SOURCE"/*.txt 2>/dev/null
  echo "[OK] Fichiers .txt de data/source nettoyés."
  
  # Copier les images de test vers le dossier source
  if [ -d "$TEST_IMAGES" ]; then
    cp -f "$TEST_IMAGES"/* "$SOURCE"/ 2>/dev/null
    echo "[OK] Images de test copiées dans data/source."
  else
    echo "[WARN] Dossier data/test-images introuvable."
  fi
else
  echo "[INFO] data/source n'existe pas."
fi

echo "========================================="
echo "Nettoyage terminé. Le producer peut être relancé."
