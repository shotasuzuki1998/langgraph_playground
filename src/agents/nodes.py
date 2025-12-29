"""
LangGraphノード定義
"""

import json

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from src.agents.evidence import EvidenceGraph
from src.agents.graph_builder import build_evidence_graph
from src.agents.state import AgentState
from src.external.db.session import execute_sql
from src.schemas.database_schema import SCHEMA_INFO
from src.services.query_checker import check_query
from src.settings import settings


def get_llm():
    return ChatOpenAI(
        model=settings.llm_model,
        temperature=0,
        api_key=settings.openai_api_key if settings.openai_api_key else None,
    )


def generate_sql_node(state: AgentState) -> AgentState:
    """SQLを生成"""
    retry_context = ""
    if state.get("error") and state.get("retry_count", 0) > 0:
        error_type = state.get("error_type", "unknown")
        if error_type == "check":
            retry_context = f"""
【前回のエラー - SQLポリシー違反】
生成したSQL: {state.get('sql_query', '')}
エラー: {state['error']}

ポリシーに準拠したSQLを生成してください。
"""
        else:
            retry_context = f"""
【前回のエラー - SQL実行エラー】
生成したSQL: {state.get('sql_query', '')}
エラー: {state.get('error', '')}

構文エラーを修正してください。
"""

    system_prompt = f"""あなたはGoogle広告データベースのSQLエキスパートです。
ユーザーの質問に対して、適切なSQLクエリを生成してください。

{SCHEMA_INFO}

## 絶対に守るべきルール
- SELECTクエリのみ生成
- MySQL構文を使用
- 日付は 'YYYY-MM-DD' 形式
- 集計時はGROUP BYを忘れずに
- LIMITは自動追加されるので不要
- SQLのみを出力（説明不要、マークダウン不要）
"""

    user_prompt = f"{retry_context}質問: {state['question']}"

    llm = get_llm()
    response = llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
    )

    sql = response.content.strip()
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    return {**state, "sql_query": sql, "error": None, "error_type": None}


def check_query_node(state: AgentState) -> AgentState:
    """SQLをチェック"""
    result = check_query(state["sql_query"])

    if result.is_valid:
        return {**state, "checked_query": result.query, "error": None, "error_type": None}
    else:
        return {
            **state,
            "checked_query": "",
            "error": result.error,
            "error_type": "check",
            "retry_count": state.get("retry_count", 0) + 1,
        }


def execute_sql_node(state: AgentState) -> AgentState:
    """SQLを実行"""
    result = execute_sql(state["checked_query"])

    if result["success"]:
        formatted = f"結果: {result['row_count']}件\n{json.dumps(result['data'], ensure_ascii=False, default=str)}"
        return {**state, "sql_result": formatted, "error": None, "error_type": None}
    else:
        return {
            **state,
            "sql_result": "",
            "error": result["error"],
            "error_type": "execute",
            "retry_count": state.get("retry_count", 0) + 1,
        }


def build_evidence_graph_node(state: AgentState) -> AgentState:
    """SQL結果からEvidence Graphを構築"""
    graph = build_evidence_graph(
        sql_result=state["sql_result"], sql_query=state["checked_query"], question=state["question"]
    )
    return {**state, "evidence_graph": graph}


def reason_with_graph_node(state: AgentState) -> AgentState:
    """Evidence Graphを使って推論・回答生成"""
    graph: EvidenceGraph = state["evidence_graph"]

    evidence_prompt = graph.to_reasoner_prompt()

    system_prompt = """あなたはGoogle広告のデータアナリストです。
以下の「Evidence Graph」（構造化されたエビデンス）に基づいて、質問に回答してください。

## 回答ルール
1. Evidence Graphに含まれるエビデンスのみを使用
2. 数値は必ずエビデンスから引用
3. 集計値を優先的に活用
4. 結論を最初に述べる
5. 数値はカンマ区切りで見やすく
"""

    user_prompt = f"""{evidence_prompt}

---

上記のEvidence Graphに基づいて回答してください。
"""

    llm = get_llm()
    response = llm.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
    )

    return {**state, "answer": response.content}


def handle_error_node(state: AgentState) -> AgentState:
    """エラーハンドリング"""
    error_msg = f"""申し訳ありません。クエリの実行に失敗しました。

エラー: {state.get('error', '不明')}
試行したSQL: {state.get('sql_query', 'なし')}
リトライ回数: {state.get('retry_count')}/{settings.max_retries}

質問を変えて再度お試しください。"""

    return {**state, "answer": error_msg}


def check_query_result(state: AgentState) -> str:
    """クエリチェック結果を判定"""
    if state.get("error"):
        if state.get("retry_count", 0) < settings.max_retries:
            return "retry"
        return "error"
    return "success"


def check_execute_result(state: AgentState) -> str:
    """SQL実行結果を判定"""
    if state.get("error"):
        if state.get("retry_count", 0) < settings.max_retries:
            return "retry"
        return "error"
    return "success"
