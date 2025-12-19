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
    """
    SQLを実行して結果を返す

    Args:
        query: 実行するSQLクエリ（チェック済みを想定）
        max_rows: 取得する最大行数

    Returns:
        dict: 実行結果
            - success: 成功/失敗
            - columns: カラム名リスト
            - data: 行データリスト
            - row_count: 取得行数
            - error: エラーメッセージ（失敗時）
    """
    max_rows = min(max_rows, settings.max_limit)

    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            columns = list(result.keys())
            rows = result.fetchmany(max_rows)
            data = [dict(zip(columns, row)) for row in rows]

            return {
                "success": True,
                "columns": columns,
                "data": data,
                "row_count": len(data),
            }
    except SQLAlchemyError as e:
        return {"success": False, "error": str(e)}


def format_result(result: dict) -> str:
    """
    結果をテーブル形式でフォーマット

    Args:
        result: execute_sqlの戻り値

    Returns:
        str: フォーマットされた結果文字列
    """
    if not result["success"]:
        return f"エラー: {result['error']}"

    if not result["data"]:
        return "結果: 0件"

    columns = result["columns"]
    data = result["data"]

    # カラム幅を計算
    widths = {col: len(str(col)) for col in columns}
    for row in data:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))[:40]))

    # テーブル作成
    header = " | ".join(str(col).ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)
    rows_str = "\n".join(
        " | ".join(str(row.get(col, ""))[:40].ljust(widths[col]) for col in columns) for row in data
    )

    return f"結果: {result['row_count']}件\n\n{header}\n{separator}\n{rows_str}"
