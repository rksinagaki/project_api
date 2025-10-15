# src/app/app_faq.py
import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import json
import re
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st
from sklearn.neighbors import NearestNeighbors
from sentence_transformers import SentenceTransformer

# ============= 基本設定 =============
st.set_page_config(page_title="YouTube コメント FAQ 生成", layout="wide")

BASE = Path(__file__).resolve().parent
OUT = (BASE / "../notebook/outputs").resolve()
EMB_PATH = OUT / "embeddings.npy"
CORPUS_PATH = OUT / "corpus_df.pkl"
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# ============= ロード処理 =============
@st.cache_resource
def load_all():
    embeddings = np.load(EMB_PATH)
    corpus_df = pd.read_pickle(CORPUS_PATH)
    nn = NearestNeighbors(n_neighbors=50, metric="cosine").fit(embeddings)
    enc = SentenceTransformer(MODEL_NAME, device="cpu")
    return corpus_df, nn, enc

try:
    corpus_df, nn, enc_model = load_all()
except Exception as e:
    st.error(f"データ読み込み失敗: {e}\n期待する場所: {OUT}")
    st.stop()

# ============= 検索関数 =============
def search(query: str, top_k: int = 10) -> pd.DataFrame:
    q_vec = enc_model.encode([query], convert_to_numpy=True, normalize_embeddings=True)
    dist, idx = nn.kneighbors(q_vec, n_neighbors=top_k)
    sims = (1.0 - dist[0]).round(4)
    rows = corpus_df.iloc[idx[0]].copy()
    rows.insert(0, "similarity", sims)
    return rows[["similarity", "title", "clean_text", "video_id"]]

# ============= UI =============
st.title("🧩 YouTube コメントから FAQ 自動生成（RAG）")

with st.sidebar:
    st.markdown("### 設定")
    top_k = st.slider("検索で使うコメント数 (Top-K)", 5, 50, 15, step=5)
    n_faq = st.slider("生成するFAQの数", 3, 12, 6)
    st.caption("※ embeddings.npy / corpus_df.pkl は `src/notebook/outputs/` に配置")

query = st.text_input("FAQを作りたいテーマ（例：泣ける曲、ライブ情報、人気曲の理由 など）", "")
btn = st.button("検索してFAQを作る")

# ============= 実行 =============
if btn and query.strip():
    hits = search(query, top_k=top_k)
    st.subheader("🔎 参照コメント（抜粋）")
    st.dataframe(hits, use_container_width=True, height=320)

    # ============ Gemini で FAQ 生成 ============
    from dotenv import load_dotenv
    load_dotenv()
    api_key = os.getenv("GOOGLE_API_KEY")

    if not api_key:
        st.warning("GOOGLE_API_KEY が未設定です。.env に設定してから再実行してください。")
        st.stop()

    import google.generativeai as genai
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        "models/gemini-2.5-flash",
        generation_config={"response_mime_type": "application/json"},
    )

    context = "\n".join(hits["clean_text"].tolist())[:12000]

    prompt = f"""
あなたはYouTubeコメントからFAQ（質問と回答）を要約・生成するアシスタントです。
以下の条件で日本語のFAQを{n_faq}件作成してください。

- 形式はJSON配列（例: [{{"question": "...", "answer": "...", "evidence": ["...","..."]}}]）
- 回答はコメント内容に基づく要約とし、根拠コメントを evidence に3件以内で添える。
- JSON以外の文章は出力しない。

【参照コメント（抜粋）】
{context}
"""

    with st.spinner("Gemini がFAQを生成中…"):
        resp = model.generate_content(prompt)

    text = getattr(resp, "text", str(resp))

    # JSONパース（柔軟に対応）
    try:
        faqs = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"```json\s*($begin:math:display$.*?$end:math:display$)\s*```", text, re.S)
        if match:
            faqs = json.loads(match.group(1))
        else:
            faqs = None

    st.subheader("🧠 生成されたFAQ")
    if isinstance(faqs, list):
        for i, qa in enumerate(faqs[:n_faq], 1):
            q = qa.get("question", "")
            a = qa.get("answer", "")
            ev = qa.get("evidence", [])
            st.markdown(f"**Q{i}. {q}**")
            st.write(a)
            if ev:
                with st.expander("根拠（コメント抜粋）"):
                    for e in ev:
                        st.write(f"- {e}")
            st.markdown("---")
    else:
        st.info("⚠️ JSON解析に失敗したため、生成結果をそのまま表示します。")
        st.code(text, language="json")

    st.caption(f"読み込み元: {OUT}")