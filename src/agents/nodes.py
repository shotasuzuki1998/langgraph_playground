"""
LangGraphノード定義
各処理ステップを関数として定義
"""

import json

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from src.agents.state import AgentState
from src.external.db.session import execute_sql
from src.schemas.database_schema import SCHEMA_INFO
from src.services.query_checker import check_query
from src.settings import settings


def get_llm():
    """
    LLMインスタンスを取得

    Returns:
        ChatOpenAI: OpenAI LLMインスタンス
    """
    return ChatOpenAI(
        model=settings.llm_model,
        temperature=0,
        api_key=settings.openai_api_key if settings.openai_api_key else None,
    )


def generate_sql_node(state: AgentState) -> AgentState:
    """
    SQLを生成するノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（sql_queryが設定される）
    """
    # エラー時のリトライプロンプト
    retry_context = ""
    if state.get("error") and state.get("retry_count", 0) > 0:
        error_type = state.get("error_type", "unknown")
        if error_type == "check":
            retry_context = f"""
【前回のエラー - SQLポリシー違反】
生成したSQL: {state.get('sql_query', '')}
エラー: {state['error']}

ポリシーに準拠したSQLを生成してください。
- SELECT文のみ使用可能
- サブクエリ、UNION、WITH句は使用不可
- 許可されたテーブルのみアクセス可能
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

## 注意事項
- 集計関数（SUM, AVG等）とサブクエリを比較する場合は、計算方法を一致させる
- NG例: HAVING SUM(cost)/SUM(clicks) = (SELECT MAX(cost/clicks) FROM ...)
  → 外側は「全期間平均」、内側は「日別MAX」で計算方法が違う
- OK例: HAVING SUM(cost)/SUM(clicks) = (SELECT MAX(avg_cpc) FROM (SELECT SUM(cost)/SUM(clicks) AS avg_cpc FROM ... GROUP BY keyword_id) t)
  → 両方とも「全期間平均」で統一

## 「〇〇ごとのTOP N」への対応
全件取得してORDER BYで並べてください。
グループごとの抽出は回答生成時に行います。

例: キャンペーンごとのCPC最大
→ SELECT c.name, k.keyword_text, SUM(cost)/NULLIF(SUM(clicks),0) AS cpc 
   FROM ... GROUP BY c.id, k.id ORDER BY cpc DESC
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
    # マークダウンのコードブロックを除去
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    return {**state, "sql_query": sql, "error": None, "error_type": None}


def check_query_node(state: AgentState) -> AgentState:
    """
    SQLをチェックするノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（checked_queryまたはerrorが設定される）
    """
    result = check_query(state["sql_query"])

    if result.is_valid:
        return {
            **state,
            "checked_query": result.query,
            "error": None,
            "error_type": None,
        }
    else:
        return {
            **state,
            "checked_query": "",
            "error": result.error,
            "error_type": "check",
            "retry_count": state.get("retry_count", 0) + 1,
        }


def execute_sql_node(state: AgentState) -> AgentState:
    """
    SQLを実行するノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（sql_resultまたはerrorが設定される）
    """
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


def generate_answer_node(state: AgentState) -> AgentState:
    """
    回答を生成するノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（answerが設定される）
    """
    prompt = f"""以下のSQL実行結果をもとに、ユーザーの質問に回答してください。

【質問】
{state['question']}

【実行したSQL】
{state['checked_query']}

【実行結果】
{state['sql_result']}

## 回答ルール
- 数値の小数点(.)は小数を表します（桁区切りではありません）
- 金額が1000以上の場合のみカンマ区切りで表示。
- 必要に応じて考察も加えてください。
"""

    llm = get_llm()
    response = llm.invoke(
        [
            SystemMessage(content="あなたはGoogle広告のデータアナリストです。"),
            HumanMessage(content=prompt),
        ]
    )

    return {**state, "answer": response.content}


def handle_error_node(state: AgentState) -> AgentState:
    """
    エラーハンドリングノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（answerにエラーメッセージが設定される）
    """
    error_msg = f"""申し訳ありません。クエリの実行に失敗しました。

エラー: {state.get('error', '不明')}
試行したSQL: {state.get('sql_query', 'なし')}
リトライ回数: {state.get('retry_count')}/{settings.max_retries}

質問を変えて再度お試しください。"""

    return {**state, "answer": error_msg}


# 以下条件分岐関数
def check_query_result(state: AgentState) -> str:
    """
    クエリチェック結果を判定

    Args:
        state: 現在のエージェント状態

    Returns:
        str: 次のノード名（"success", "retry", "error"）
    """
    if state.get("error"):
        if state.get("retry_count", 0) < settings.max_retries:
            return "retry"
        return "error"
    return "success"


def check_execute_result(state: AgentState) -> str:
    """
    SQL実行結果を判定

    Args:
        state: 現在のエージェント状態

    Returns:
        str: 次のノード名（"success", "retry", "error"）
    """
    if state.get("error"):
        if state.get("retry_count", 0) < settings.max_retries:
            return "retry"
        return "error"
    return "success"
