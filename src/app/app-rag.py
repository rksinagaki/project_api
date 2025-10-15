# app-rag.py —— YouTube コメント RAG（検索＋生成）

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from sklearn.neighbors import NearestNeighbors
from sentence_transformers import SentenceTransformer

# ---------- 基本設定 ----------
st.set_page_config(page_title="YouTube RAG Demo", layout="wide")

BASE = Path(__file__).resolve().parent
OUT = (BASE / "../notebook/outputs").resolve()
EMB_PATH = OUT / "embeddings.npy"
CORPUS_PATH = OUT / "corpus_df.pkl"

# ---------- データロード ----------
@st.cache_resource
def load_all():
    embeddings = np.load(EMB_PATH)
    corpus_df = pd.read_pickle(CORPUS_PATH)
    nn = NearestNeighbors(n_neighbors=50, metric="cosine").fit(embeddings)
    enc_model = SentenceTransformer(
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        device="cpu"
    )
    return corpus_df, nn, enc_model

corpus_df, nn, enc_model = load_all()

# ---------- 検索関数 ----------
def search(query: str, top_k: int = 5):
    q_vec = enc_model.encode([query], convert_to_numpy=True, normalize_embeddings=True)
    dist, idx = nn.kneighbors(q_vec, n_neighbors=top_k)
    sims = 1.0 - dist[0]
    rows = corpus_df.iloc[idx[0]].copy()
    rows.insert(0, "similarity", sims.round(4))
    return rows[["similarity", "title", "clean_text", "video_id"]]

# ---------- UI ----------
st.title("🎬 YouTube コメント RAG（検索＋生成）")

query = st.text_input("質問（例: 泣ける曲を教えて）", "")
top_k = st.slider("Top-K", 3, 20, 5)
do_generate = st.checkbox("Gemini で回答も生成する", value=False)

if st.button("検索") and query.strip():
    hits = search(query, top_k=top_k)
    st.subheader("🔍 検索結果")
    st.dataframe(hits, use_container_width=True, height=360)

    # ---------- Gemini連携 ----------
    if do_generate:
        import google.generativeai as genai
        from dotenv import load_dotenv

        load_dotenv()
        genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

        context = "\n".join(hits["clean_text"].tolist())[:12000]
        prompt = f"""
        以下のYouTubeコメントを参考に質問に答えてください。
        質問: {query}
        コメント:
        {context}
        """

        with st.spinner("Gemini が回答生成中…"):
            llm = genai.GenerativeModel("models/gemini-2.5-flash")
            resp = llm.generate_content(prompt)

        st.subheader("🧠 生成結果")
        st.write(resp.text)