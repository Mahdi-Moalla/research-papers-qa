class CONFIG:
    DB_FILE="data.duckdb"
    QDRANT_SERVER_URL="http://localhost:6333"
    QDRANT_COLLECTION_NAME="research-papers-qa"
    EMBEDDING_MODEL="jinaai/jina-embeddings-v2-small-en"
    TEXT_SPLIT_PARAMS=dict(
        # Set a really small chunk size, just to show.
        separators=["\n\n", "\n", "."],
        chunk_size=1000,
        chunk_overlap=500,
        length_function=len,
        is_separator_regex=True,
    )
    LLM_MODEL="deepseek/deepseek-chat-v3.1:free"
    LLM_BASE_URL="https://openrouter.ai/api/v1"
    #"api_key":userdata.get('openrouter_key'),
    LLM_MODEL_PROVIDER="openai"
