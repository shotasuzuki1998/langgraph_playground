"""
Google広告 SQLエージェント

フロー:
  質問 → SQL生成 → チェック → 実行 → Evidence構築 → 推論 → 回答
"""

from langgraph.graph import END, StateGraph

from src.agents.nodes import (
    build_evidence_node,
    check_execute_result,
    check_query_node,
    check_query_result,
    execute_sql_node,
    generate_sql_node,
    handle_error_node,
    reason_with_evidence_node,
)
from src.agents.state import AgentState


def build_graph():
    """
    LangGraphのワークフローを構築

    ワークフロー:
    1. generate_sql: 自然言語からSQLを生成
    2. check_query: SQLの安全性をチェック
    3. execute_sql: SQLを実行
    4. build_evidence: Evidenceを構築
    5. reason_with_evidence: Evidenceベースで推論

    Returns:
        CompiledGraph: コンパイル済みのLangGraphワークフロー
    """
    workflow = StateGraph(AgentState)

    # ノード追加
    workflow.add_node("generate_sql", generate_sql_node)
    workflow.add_node("check_query", check_query_node)
    workflow.add_node("execute_sql", execute_sql_node)
    workflow.add_node("build_evidence", build_evidence_node)
    workflow.add_node("reason_with_evidence", reason_with_evidence_node)
    workflow.add_node("handle_error", handle_error_node)

    # エントリーポイント
    workflow.set_entry_point("generate_sql")

    # エッジ
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

    # 条件分岐: SQL実行後
    workflow.add_conditional_edges(
        "execute_sql",
        check_execute_result,
        {
            "success": "build_evidence",
            "retry": "generate_sql",
            "error": "handle_error",
        },
    )

    # Evidence構築 → 推論
    workflow.add_edge("build_evidence", "reason_with_evidence")
    workflow.add_edge("reason_with_evidence", END)
    workflow.add_edge("handle_error", END)

    return workflow.compile()


# グラフをコンパイル
agent = build_graph()


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
        "evidence": None,
        "answer": "",
        "error": None,
        "error_type": None,
        "retry_count": 0,
    }

    result = agent.invoke(initial_state)

    output = {
        "question": result["question"],
        "sql_query": result["sql_query"],
        "checked_query": result["checked_query"],
        "sql_result": result["sql_result"],
        "answer": result["answer"],
        "error": result.get("error"),
    }

    if result.get("evidence"):
        evidence = result["evidence"]
        output["evidence"] = evidence.to_dict()
        output["evidence_prompt"] = evidence.to_prompt()

    return output
