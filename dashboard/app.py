import streamlit as st
import pandas as pd
import time
import os
import sys
import re
import urllib.parse

st.set_page_config(page_title="Distracted Driver Detection Dashboard", layout="wide")

st.title("👁️ Real-time Distracted Driver Detection Dashboard")
st.markdown("---")

# Configuration de rafraîchissement
st.sidebar.header("Paramètres")
auto_refresh = st.sidebar.checkbox("Auto-refresh (chaque 2s)", value=True)

# 10 Classes de distraction
classes = {
    0: "c0: Conduite normale",
    1: "c1: SMS au volant (Droit)",
    2: "c2: Téléphone (Droit)",
    3: "c3: SMS au volant (Gauche)",
    4: "c4: Téléphone (Gauche)",
    5: "c5: Réglage Radio",
    6: "c6: En train de Boire",
    7: "c7: Se retourner derrière",
    8: "c8: Maquillage",
    9: "c9: Parler au passager"
}

# Fonction pour convertir les chemins Spark (locaux ou WSL) vers des chemins locaux exploitables par Streamlit
def convert_spark_path(spark_path):
    if not isinstance(spark_path, str):
        return None
        
    # Décoder les caractères d'URL (%20 pour les espaces, etc.)
    path = urllib.parse.unquote(spark_path)
    
    # Supprimer le préfixe 'file:'
    if path.startswith("file:"):
        path = path[5:]
        
    # Nettoyer les slashes répétés au début (ex: ///mnt/c/ -> /mnt/c/)
    path = re.sub(r'^/+', '/', path)
    
    # Détecter si on tourne sous Linux (WSL) ou Windows
    is_linux = sys.platform.startswith('linux') or os.name == 'posix'
    
    if is_linux:
        # Si on est sous Linux, on doit s'assurer d'utiliser les chemins de montage WSL /mnt/...
        if path.startswith("C:/") or path.startswith("c:/"):
            path = "/mnt/c/" + path[3:]
        elif re.match(r'^[a-zA-Z]:/', path):
            path = f"/mnt/{path[0].lower()}/" + path[3:]
        # Rendre absolu si relatif
        if not path.startswith("/"):
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            path = os.path.join(project_root, path)
        path = path.replace('\\', '/')
    else:
        # Si on est sous Windows, on convertit le montage WSL en lettre de lecteur
        if path.startswith("/mnt/c/"):
            path = "C:/" + path[7:]
        elif re.match(r'^/[a-zA-Z]/', path):
            path = path[1] + ":" + path[2:]
        # Rendre absolu si relatif
        if not (len(path) > 1 and path[1] == ":") and not os.path.isabs(path):
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            path = os.path.join(project_root, path)
        path = os.path.normpath(path)
        
    return path

# Chemin vers predictions.csv
predictions_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "output", "predictions.csv")

if os.path.exists(predictions_file):
    try:
        df = pd.read_csv(predictions_file)
        
        if not df.empty and 'image' in df.columns and 'prediction' in df.columns:
            # Conversion de la prédiction en entier
            df['prediction_int'] = df['prediction'].astype(int)
            df['class_name'] = df['prediction_int'].map(classes)
            
            # Dernières statistiques
            total_images = len(df)
            last_row = df.iloc[-1]
            last_image_spark = last_row['image']
            last_pred_int = int(last_row['prediction'])
            last_pred_class = classes.get(last_pred_int, "Inconnu")
            
            # Affichage des KPI
            col1, col2 = st.columns(2)
            col1.metric("Images analysées en continu", f"{total_images:,}")
            
            with col2:
                if last_pred_int == 0:
                    st.success("✅ Conducteur attentif (Conduite normale)")
                else:
                    st.error(f"🚨 ATTENTION : {last_pred_class.split(': ')[1]}")
            
            st.markdown("---")
            
            # Disposition principale
            main_col1, main_col2 = st.columns([1, 1])
            
            # Colonne 1 : Affichage en direct de l'image traitée
            with main_col1:
                st.subheader("Dernière image traitée")
                local_img_path = convert_spark_path(last_image_spark)
                if local_img_path and os.path.exists(local_img_path):
                    st.image(local_img_path, caption=f"Fichier : {os.path.basename(local_img_path)} — Classe prédite : {last_pred_class}", width='stretch')
                else:
                    st.info(f"Image en attente d'affichage... (Chemin détecté : {local_img_path})")
            
            # Colonne 2 : Graphique statistique
            with main_col2:
                st.subheader("Statistiques globales de détections")
                counts = df['class_name'].value_counts().reset_index()
                counts.columns = ['Classe', 'Nombre']
                
                # Trier par numéro de classe c0, c1, ...
                counts['sort_key'] = counts['Classe'].apply(lambda x: int(x.split(':')[0][1:]))
                counts = counts.sort_values(by='sort_key').drop('sort_key', axis=1)
                
                st.bar_chart(data=counts, x='Classe', y='Nombre', use_container_width=True)
            
            st.markdown("---")
            st.subheader("🖼️ Galerie des dernières détections (Historique)")
            
            # Prendre les 8 dernières détections (les plus récentes en premier)
            recent_df = df.iloc[::-1].head(8)
            
            # Créer une grille de 4 colonnes
            cols = st.columns(4)
            for idx, (_, row) in enumerate(recent_df.iterrows()):
                col = cols[idx % 4]
                img_path_raw = row['image']
                pred_int = int(row['prediction'])
                pred_label = classes.get(pred_int, "Inconnu")
                
                local_path = convert_spark_path(img_path_raw)
                with col:
                    if local_path and os.path.exists(local_path):
                        status_emoji = "✅" if pred_int == 0 else "🚨"
                        short_desc = pred_label.split(': ')[1]
                        st.image(local_path, width='stretch')
                        st.markdown(f"**{status_emoji} {short_desc}**")
                        st.caption(f"`{os.path.basename(local_path)}`")
                    else:
                        st.caption(f"Image non trouvée : `{os.path.basename(str(img_path_raw))}`")
            
            st.markdown("---")
            with st.expander("Historique complet des détections (Tableau brut)"):
                st.dataframe(df[['image', 'class_name']], use_container_width=True)
                
        else:
            st.info("predictions.csv est vide ou n'a pas les colonnes requises.")
            
    except Exception as e:
        st.warning(f"Erreur de lecture ou fichier partiellement écrit : {e}")
else:
    st.info("⏳ En attente de données... Aucun fichier trouvé dans `data/output/predictions.csv`.\n\nLancez le Consumer Spark et démarrez le Producer pour simuler le flux d'images.")

if auto_refresh:
    time.sleep(2)
    st.rerun()
