"""
データベース接続・SQL実行モジュール
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.settings import settings


def get_db_engine():
    """
    データベースエンジンを取得

    Returns:
        Engine: SQLAlchemyエンジン
    """
    connection_string = (
        f"mysql+pymysql://{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )
    return create_engine(connection_string, pool_pre_ping=True)


def execute_sql(query: str, max_rows: int = settings.default_limit) -> dict:
    max_rows = min(max_rows, settings.max_limit)

    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            results = conn.execute(text(query)).mappings().fetchmany(max_rows)
            data = [dict(result) for result in results]

            return {
                "success": True,
                "data": data,
                "row_count": len(data),
            }
    except SQLAlchemyError as e:
        return {"success": False, "error": str(e)}
