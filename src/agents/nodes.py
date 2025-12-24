"""
LangGraphノード定義
各処理ステップを関数として定義
"""

import asyncio
import json

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from src.agents.state import AgentState
from src.external.db.session import execute_sql
from src.external.weather.open_meteo_client import get_weather
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

## ルール
- SELECTクエリのみ生成
- MySQL構文を使用
- 日付は 'YYYY-MM-DD' 形式
- 集計時はGROUP BYを忘れずに
- サブクエリ、UNION、WITH句（CTE）は使用禁止
- LIMITは自動で追加されるので不要
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

数値はカンマ区切りで見やすく、必要に応じて考察も加えてください。
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


def check_weather_needed_node(state: AgentState) -> AgentState:
    """
    天気情報が必要かどうかを判定するノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（needs_weather, weather_locationsが設定される）
    """
    question = state["question"].lower()

    # 天気関連のキーワード
    weather_keywords = ["天気", "気温", "weather", "温度"]

    # 場所のキーワード
    location_patterns = {
        "tokyo": ["東京", "tokyo", "とうきょう"],
        "osaka": ["大阪", "osaka", "おおさか"],
    }

    # 天気キーワードが含まれているか確認
    needs_weather = any(keyword in question for keyword in weather_keywords)

    # 場所を特定
    locations = []
    if needs_weather:
        for location_key, patterns in location_patterns.items():
            if any(pattern in question for pattern in patterns):
                locations.append(location_key)

        # 場所が指定されていない場合は両方取得
        if not locations:
            locations = ["tokyo", "osaka"]

    return {
        **state,
        "needs_weather": needs_weather,
        "weather_locations": locations,
        "weather_info": [],
        "weather_api_history": {
            "called": False,
            "locations": [],
            "success": False,
            "error": None,
        },
    }


# ここから天気関連のnode
def fetch_weather_node(state: AgentState) -> AgentState:
    """
    天気情報を取得するノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（weather_info, weather_api_historyが設定される）
    """
    locations = state.get("weather_locations", [])

    if not locations:
        return {
            **state,
            "weather_info": [],
            "weather_api_history": {
                "called": False,
                "locations": [],
                "success": True,
                "error": None,
            },
        }

    # 非同期処理を同期的に実行
    async def fetch_all():
        tasks = [get_weather(loc) for loc in locations]
        return await asyncio.gather(*tasks)

    try:
        results = asyncio.run(fetch_all())

        weather_info = []
        all_success = True
        errors = []

        for result in results:
            weather_info.append(
                {
                    "location": result["location"],
                    "temperature": result["temperature"],
                    "weather_code": result["weather_code"],
                    "weather_description": result["weather_description"],
                }
            )
            if not result["success"]:
                all_success = False
                if result["error"]:
                    errors.append(result["error"])

        return {
            **state,
            "weather_info": weather_info,
            "weather_api_history": {
                "called": True,
                "locations": locations,
                "success": all_success,
                "error": "; ".join(errors) if errors else None,
            },
        }
    except Exception as e:
        return {
            **state,
            "weather_info": [],
            "weather_api_history": {
                "called": True,
                "locations": locations,
                "success": False,
                "error": str(e),
            },
        }


def generate_answer_with_weather_node(state: AgentState) -> AgentState:
    """
    天気情報を含めて回答を生成するノード

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（answerが設定される）
    """
    # 天気情報のフォーマット
    weather_text = ""
    if state.get("weather_info"):
        weather_parts = []
        for info in state["weather_info"]:
            if info["temperature"] is not None:
                weather_parts.append(
                    f"{info['location']}: {info['weather_description']}、気温 {info['temperature']}°C"
                )
        if weather_parts:
            weather_text = "\n\n【現在の天気】\n" + "\n".join(weather_parts)

    prompt = f"""以下のSQL実行結果をもとに、ユーザーの質問に回答してください。

【質問】
{state['question']}

【実行したSQL】
{state['checked_query']}

【実行結果】
{state['sql_result']}
{weather_text}

数値はカンマ区切りで見やすく、必要に応じて考察も加えてください。
天気情報がある場合は、それも回答に含めてください。
"""

    llm = get_llm()
    response = llm.invoke(
        [
            SystemMessage(content="あなたはGoogle広告のデータアナリストです。"),
            HumanMessage(content=prompt),
        ]
    )

    return {**state, "answer": response.content}


# 条件分岐関数を追加
def should_fetch_weather(state: AgentState) -> str:
    """
    天気取得が必要かどうかを判定

    Args:
        state: 現在のエージェント状態

    Returns:
        str: 次のノード名（"fetch_weather" or "generate_answer"）
    """
    if state.get("needs_weather") and state.get("weather_locations"):
        return "fetch_weather"
    return "generate_answer"
