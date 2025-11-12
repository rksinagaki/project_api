import os
from dotenv import load_dotenv
import streamlit as st
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from langchain_core.prompts import ChatPromptTemplate # コンテキストの拡張で追加したやつ
from langchain_chroma import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from operator import itemgetter
from langchain_core.output_parsers import StrOutputParser
import base64

# .envから読み込む
load_dotenv()
api_key = os.getenv("GOOGLE_API_KEY")

# 画像の説明を取得
def get_image_description(image_data):
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "human",
                [
                    {
                        "type": "image_url",
                        "image_url":{"url":f"data:image/jpeg;base64,{image_data}"},
                    }
                ],
            ),
        ]
    )
    chain = prompt | ChatGoogleGenerativeAI(model="gemini-2.5-flash",google_api_key=api_key) | StrOutputParser()
    return chain.invoke({"image_data": image_data})

# メッセージを作成
def create_message(dic):
    image_data = dic["image"]
    if image_data:
        return[
            (
                "human",
                [
                    {"type": "text", "text": dic["input"]},
                    {
                        "type": "image_url",
                        "image_url":{"url":f"data:image/jpeg;base64,{image_data}"},
                    },
                ],
            )
        ]
    return [("human", dic["input"])]
        
                
                        

# ドキュメントを整形
def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)
    
# チェーンを作成
def create_chain():
    vectorstore = Chroma(
        embedding_function=GoogleGenerativeAIEmbeddings(model="models/embedding-001"),
        persist_directory="/work/src/app/data/hino_trash",
    )
    retriever = vectorstore.as_retriever(search_kwargs={"k": 3})
    
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "回答には以下の情報も参考にしてください。参考情報：\n{info}",
            ),
            ("placeholder", "{history}"),
            ("placeholder", "{message}"),
        ]
    )
    

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash", google_api_key=api_key, temperature=0
    )

    chain = {
        "message": create_message,
        "info": itemgetter("input") | retriever | format_docs,
        "history": itemgetter("history"),
    } | prompt | llm

    return chain
    
# セッション状態を初期化
if "history" not in st.session_state:
    st.session_state.history = []
    st.session_state.chain = create_chain()
    
st.title("マルチモーダルRAGチャットボット")

# アップローダーを追加
uploaded_file = st.file_uploader("画像を選択してください", type=["jpg", "jpeg", "png"])

# アップロードされた画像を表示
if uploaded_file is not None:
    st.image(uploaded_file, caption="画像", width=300)  

# ユーザ入力を受け取る
user_input = st.text_input("メッセージを入力してください:")

# ボタンを追加してクリックされたらアクションを起こす
if st.button("送信"):
    # クリック時のアクションを変更してみる
    image_data = None
    image_description = ""
    if uploaded_file is not None:
        image_data = base64.b64encode(uploaded_file.read()).decode("utf-8")
        image_description = get_image_description(image_data)
        
    response = st.session_state.chain.invoke(
        {
            
            "input":user_input + image_description,
            "history":st.session_state.history,
            "image":image_data,
        }
    )
    st.session_state.history.append(HumanMessage(user_input))
    st.session_state.history.append(response)    

    # 会話を表示
    for message in reversed(st.session_state.history):
        st.write(f"{message.type}: {message.content}")
        