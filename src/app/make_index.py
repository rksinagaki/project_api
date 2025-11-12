from langchain_community.document_loaders import CSVLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings


def load_document(filename):
    # CSVをロード
    loader = CSVLoader(file_path=filename, autodetect_encoding=True)
    pages = loader.load()

    # テキストを分割
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=400)
    splits = text_splitter.split_documents(pages)

    # 埋め込みモデルを定義（Gemini埋め込み）
    embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

    # Chromaベクトルストアを作成・保存
    db = Chroma.from_documents(
        documents=splits,
        embedding=embeddings,
        persist_directory="data/hino_trash"  # ベクトルデータを保存する場所
    )
    print("インデックス作成完了")

# ← ここから下がスクリプト直実行時の入り口
if __name__ == "__main__":
    # ここでCSVの場所を指定（相対パス or 絶対パス）
    csv_path = "../../data/hino_trash/hino_trash.csv",
               "../../data/hino_trash/hino_trash.csv" # ← 自分のファイル名に合わせて変更
    load_document(csv_path)