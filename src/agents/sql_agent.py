"""
Google広告 SQLエージェント（LangGraph版）
自然言語でGoogle広告データベースを検索できます
"""

from langgraph.graph import END, StateGraph

from src.agents.nodes import (
    check_execute_result,
    check_query_node,
    check_query_result,
    check_weather_needed_node,
    execute_sql_node,
    fetch_weather_node,
    generate_answer_node,
    generate_answer_with_weather_node,
    generate_sql_node,
    handle_error_node,
    should_fetch_weather,
)
from src.agents.state import AgentState


def build_graph():
    """
    LangGraphのワークフローを構築

    ワークフロー:
    1. check_weather_needed: 天気情報が必要か判定
    2. generate_sql: 自然言語からSQLを生成
    3. check_query: SQLの安全性をチェック
    4. execute_sql: SQLを実行
    5. (条件分岐) fetch_weather: 必要なら天気情報を取得
    6. generate_answer: 結果から回答を生成

    エラー時はリトライまたはエラーハンドリングに分岐

    Returns:
        CompiledGraph: コンパイル済みのLangGraphワークフロー
    """
    workflow = StateGraph(AgentState)

    # ノード追加
    workflow.add_node("check_weather_needed", check_weather_needed_node)
    workflow.add_node("generate_sql", generate_sql_node)
    workflow.add_node("check_query", check_query_node)
    workflow.add_node("execute_sql", execute_sql_node)
    workflow.add_node("fetch_weather", fetch_weather_node)
    workflow.add_node("generate_answer", generate_answer_node)
    workflow.add_node("generate_answer_with_weather", generate_answer_with_weather_node)
    workflow.add_node("handle_error", handle_error_node)

    # エントリーポイント
    workflow.set_entry_point("check_weather_needed")

    # エッジ
    workflow.add_edge("check_weather_needed", "generate_sql")
    workflow.add_edge("generate_sql", "check_query")

    # 条件分岐: クエリチェック後
    workflow.add_conditional_edges(
        "check_query",
        check_query_result,
        {
            "success": "execute_sql",
            "retry": "generate_sql",
            "error": "handle_error",
        },
    )

    # 条件分岐: SQL実行後 → 天気取得が必要か判定
    workflow.add_conditional_edges(
        "execute_sql",
        lambda state: (
            "error"
            if state.get("error") and state.get("retry_count", 0) >= 3
            else (
                "retry"
                if state.get("error")
                else "fetch_weather" if state.get("needs_weather") else "generate_answer"
            )
        ),
        {
            "fetch_weather": "fetch_weather",
            "generate_answer": "generate_answer",
            "retry": "generate_sql",
            "error": "handle_error",
        },
    )

    # 天気取得後は天気付き回答生成へ
    workflow.add_edge("fetch_weather", "generate_answer_with_weather")

    workflow.add_edge("generate_answer", END)
    workflow.add_edge("generate_answer_with_weather", END)
    workflow.add_edge("handle_error", END)

    return workflow.compile()


# グラフをコンパイル
agent = build_graph()


def ask(question: str) -> str:
    """
    自然言語で質問してSQLを実行し、回答を得る

    Args:
        question: 自然言語での質問

    Returns:
        str: 回答
    """
    initial_state: AgentState = {
        "question": question,
        "sql_query": "",
        "checked_query": "",
        "sql_result": "",
        "answer": "",
        "error": None,
        "error_type": None,
        "retry_count": 0,
        # 天気関連の初期値
        "needs_weather": False,
        "weather_locations": [],
        "weather_info": [],
        "weather_api_history": {
            "called": False,
            "locations": [],
            "success": False,
            "error": None,
        },
    }

    result = agent.invoke(initial_state)
    return result["answer"]


def ask_with_details(question: str) -> dict:
    """
    詳細情報付きで質問を実行

    Args:
        question: 自然言語での質問

    Returns:
        dict: 詳細情報を含む結果
    """
    initial_state: AgentState = {
        "question": question,
        "sql_query": "",
        "checked_query": "",
        "sql_result": "",
        "answer": "",
        "error": None,
        "error_type": None,
        "retry_count": 0,
        "needs_weather": False,
        "weather_locations": [],
        "weather_info": [],
        "weather_api_history": {
            "called": False,
            "locations": [],
            "success": False,
            "error": None,
        },
    }

    result = agent.invoke(initial_state)
    return {
        "question": result["question"],
        "sql_query": result["sql_query"],
        "checked_query": result["checked_query"],
        "sql_result": result["sql_result"],
        "answer": result["answer"],
        "error": result.get("error"),
        # 天気関連の詳細
        "weather_info": result.get("weather_info", []),
        "weather_api_history": result.get("weather_api_history", {}),
    }
