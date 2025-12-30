"""
エージェントの状態定義
"""

from typing import Any, TypedDict


class AgentState(TypedDict):
    """
    エージェントの状態

    Attributes:
        question: ユーザーの質問
        sql_query: 生成されたSQL
        checked_query: チェック済みSQL
        sql_result: 実行結果
        evidence: 構造化されたエビデンス
        answer: 最終回答
        error: エラーメッセージ
        error_type: エラー種別
        retry_count: リトライ回数
    """

    question: str
    sql_query: str
    checked_query: str
    sql_result: str
    evidence: Any  # Evidence
    answer: str
    error: str | None
    error_type: str | None
    retry_count: int
