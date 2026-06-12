import streamlit as st
import pandas as pd
import glob
import time
import os

st.set_page_config(page_title="Word Count Dashboard", layout="wide")

st.title("📊 Real-time Word Count Dashboard")

# Paramètres de rafraîchissement
st.sidebar.header("Paramètres")
auto_refresh = st.sidebar.checkbox("Auto-refresh (chaque 2s)", value=True)

# Chemin vers le fichier final
wordcount_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "output", "wordcount.csv")

if os.path.exists(wordcount_file):
    try:
        df = pd.read_csv(wordcount_file)
        
        if not df.empty and 'value' in df.columns and 'count' in df.columns:
            # Trier par compte décroissant
            df = df.sort_values(by="count", ascending=False)
            
            # KPI
            total_words = df['count'].sum()
            unique_words = len(df)
            top_word = df.iloc[0]['value'] if not df.empty else "N/A"
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Mots totaux traités", f"{total_words:,}")
            col2.metric("Mots uniques", f"{unique_words:,}")
            col3.metric("Mot le plus fréquent", top_word)
            
            st.divider()
            
            # Graphiques
            st.subheader("Top 20 des mots les plus fréquents")
            top_20 = df.head(20)
            
            # Utilisation de st.bar_chart pour la simplicité
            st.bar_chart(data=top_20, x='value', y='count', use_container_width=True)
            
            # Tableau des données brutes
            with st.expander("Voir les données brutes"):
                st.dataframe(df, use_container_width=True)
                
            st.caption(f"Dernière mise à jour avec le fichier : {os.path.basename(wordcount_file)}")
        else:
            st.info("Le fichier CSV est vide ou ne contient pas les colonnes attendues ('value', 'count').")
            
    except Exception as e:
        # En cas d'erreur de lecture (fichier en cours d'écriture par Spark ou erreur de parsing)
        st.warning(f"Erreur de lecture ou fichier partiellement écrit : {e}")
else:
    st.info("⏳ En attente de données... Aucun fichier trouvé dans `data/output`.\n\nAssurez-vous que le Consumer Spark est lancé et que le Producer a envoyé des fichiers.")

if auto_refresh:
    time.sleep(2)
    st.rerun()
