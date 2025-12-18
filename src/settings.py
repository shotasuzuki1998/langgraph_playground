"""
アプリケーション設定
環境変数から設定値を読み込む
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """アプリケーション設定"""

    # Database
    db_host: str = "db"
    db_port: int = 3306
    db_user: str = "root"
    db_password: str = "passwd"
    db_name: str = "llm_ad_agent"

    # LLM (OpenAI)
    llm_model: str = "gpt-4o-mini"
    openai_api_key: str = ""

    # Agent
    max_retries: int = 3
    default_limit: int = 100
    max_limit: int = 1000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # .envに他の環境変数があっても無視


settings = Settings()

