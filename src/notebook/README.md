# project_apiのディレクトリ構成

```
../project_api/
├── data/                            # データ保存フォルダ
│   |
│   └── processed/                  # 前処理済みデータや一時ファイル
│
├── docker-compose.dev.yml           # 開発環境用コンテナ設定
├── Dockerfile.dev                   # 開発用Dockerfile
├── requirements/
│   └── requirements_dev.txt         # 使用ライブラリ一覧
├── src/
│   ├── app/                         # Streamlit アプリ群（RAG・FAQ・Suggest）
│   │   ├── app-rag.py              # 検索＋生成（RAG）
│   │   ├── app-faq.py              # コメントからFAQ生成
│   │   ├── app-suggest.py          # 改善提案ボット
│   │   
│   │
│   ├── notebook/                    
│   │   ├── README.md               # Notebookの説明・構成まとめ
│   │   ├── 01_EDA_youtube_v2.ipynb # EDA・埋め込み生成
│   │   ├── outputs/                # 埋め込みデータ出力先（アプリと共有）
│   │   │   ├── embeddings.npy
│   │   │   └── corpus_df.pkl
│   │   └── figs/                   # 可視化画像・スクリーンショット
│   │       └── .png
│   │
│   └── .gitignore
│
└── .env                             # Gemini  APIキーなど環境変数
```


# 🎥 プロジェクト概要

本プロジェクトはチーム開発として実施。  
共同開発者が **YouTube Data API** と **AWS 環境** を活用し、コメントや動画メタ情報などのデータを収集・整備。  
当方がそのデータをもとに、**分析・可視化（EDA・RAG構築・Streamlitダッシュボード開発）** を担当。

目的は、YouTube コメントからユーザーの反応やトレンドを抽出し、  
アーティスト公式チャンネルの成長施策・改善提案につなげること。

- 特に担当した箇所について、YouTubeコメントデータを用いて以下を実施。
	1.	EDA（探索的データ分析）  
    コメントの分布・長さ・感情傾向などを可視化
	2.	テキスト前処理  
    正規化・不要語除去・日本語クリーニング
	3.	埋め込み生成（SentenceTransformer）  
    paraphrase-multilingual-MiniLM-L12-v2により文章ベクトル化
	4.	RAG基盤データ出力  
    embeddings.npyおよびcorpus_df.pklを作成（Streamlitで利用）

# 🚀 実行手順

1. Dockerコンテナを起動
- docker compose -f docker-compose.dev.yml up -d

2. Jupyter Labにアクセス
- http://127.0.0.1:8888

3. ノートブックを実行
- 01_EDA_youtube_v2.ipynbを開く
- 全セル実行してoutputs/に埋め込みを生成

4. Streamlitで可視化
- docker exec -it project_api-streamlit bash
- streamlit run app-rag.py --server.address=0.0.0.0 --server.port=8501
- RAG（検索＋生成）: http://127.0.0.1:8501
- FAQ生成: http://127.0.0.1:8502
- 改善提案Bot: http://127.0.0.1:8503

5. 成果物・可視化例

| **分析内容** | **説明** |
|:-------------:|:---------:|
| コメント分布 | コメントの文字数や動画ごとの件数を分析 |
| WordCloud | 頻出ワードを視覚化し、話題の傾向を確認 |
| ダッシュボード | Streamlitによるインタラクティブな可視化を実装 |

## 🚀 今後の展望
- LangChainを活用したRAG構造の再設計・拡張化   
- GCP上での展開を見据えたアーキテクチャ設計（Cloud RunやVertex AIとの統合を想定）  
- 感情分析・トレンド抽出などの自然言語処理機能の追加  
- StreamlitアプリのUX改善とLooker Studioなど外部ダッシュボードとの連携など