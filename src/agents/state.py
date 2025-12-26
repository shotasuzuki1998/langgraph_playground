"""
エージェントの状態定義
"""

from typing import TypedDict


class AgentState(TypedDict, total=False):
    """
    エージェントの状態

    Attributes:
        question: ユーザーの質問
        sql_query: 生成されたSQL
        checked_query: チェック済みSQL
        sql_result: 実行結果（フォーマット済み文字列）
        sql_result_data: 実行結果（生データ）
        answer: 最終回答
        error: エラーメッセージ
        error_type: エラー種別（"check" or "execute"）
        retry_count: リトライ回数
        needs_weather: 天気情報が必要かどうか
        weather_locations: 天気を取得する場所リスト
        weather_dates: 天気を取得する日付リスト
        weather_info: 取得した天気情報
        weather_api_history: 天気API呼び出し履歴
        weather_unavailable: 天気情報が取得できなかったかどうか
        weather_unavailable_reason: 天気情報が取得できなかった理由
        weather_note: 天気情報に関する注記（日付制限など）
    """

    question: str
    sql_query: str
    checked_query: str
    sql_result: str
    sql_result_data: list[dict]
    answer: str
    error: str | None
    error_type: str | None
    retry_count: int
    # 天気関連のstate
    needs_weather: bool
    weather_locations: list[str]
    weather_dates: list[str]
    weather_info: list[dict]
    weather_api_history: dict
    # 天気取得不可時の情報
    weather_unavailable: bool
    weather_unavailable_reason: str | None
    weather_note: str | None
