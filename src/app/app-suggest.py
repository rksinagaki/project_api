# src/app/app_suggest.py
import os, textwrap
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from sklearn.neighbors import NearestNeighbors
from sentence_transformers import SentenceTransformer

st.set_page_config(page_title="改善提案ボット (RAG)", layout="wide")

BASE = Path(__file__).resolve().parent
OUT  = (BASE / "../notebook/outputs").resolve()
EMB  = OUT / "embeddings.npy"
CORP = OUT / "corpus_df.pkl"

@st.cache_resource
def load_all():
    emb = np.load(EMB)
    df  = pd.read_pickle(CORP)
    nn  = NearestNeighbors(n_neighbors=50, metric="cosine").fit(emb)
    enc = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", device="cpu")
    return df, nn, enc

df, nn, enc = load_all()

def search(q, k=10):
    qv = enc.encode([q], convert_to_numpy=True, normalize_embeddings=True)
    dist, idx = nn.kneighbors(qv, n_neighbors=k)
    sims = (1 - dist[0]).round(4)
    rows = df.iloc[idx[0]].copy()
    rows.insert(0, "similarity", sims)
    return rows[["similarity", "title", "clean_text", "video_id"]]

st.title("🧪 改善提案ボット（RAG + Gemini）")
q = st.text_input("課題や聞きたいこと（例：チャンネル登録を増やすには？）", "")
k = st.slider("Top-K（参照コメント数）", 5, 30, 12)
go = st.button("提案を生成")

if go and q.strip():
    hits = search(q, k)
    st.subheader("🔎 参照に使うコメント（抜粋）")
    st.dataframe(hits, use_container_width=True, height=280)

    ctx = "\n".join(hits["clean_text"].astype(str).tolist())[:12000]

    from dotenv import load_dotenv
    import google.generativeai as genai
    load_dotenv()
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        st.warning("GOOGLE_API_KEY が未設定です（.env を用意）")
        st.stop()

    genai.configure(api_key=api_key)
    llm = genai.GenerativeModel("models/gemini-2.5-flash")

    prompt = textwrap.dedent(f"""
    以下のYouTubeコメントの抜粋に基づき、改善提案を日本語で箇条書きで5〜7件出してください。
    - 各提案に1行の「理由（根拠）」を添える
    - 可能なら1行で「具体アクション」も添える

    【課題】{q}

    【コメント（抜粋）】
    {ctx}
    """).strip()

    with st.spinner("Gemini が提案を生成中…"):
        resp = llm.generate_content(prompt)
    st.subheader("🧠 改善提案（RAG）")
    st.write(resp.text)