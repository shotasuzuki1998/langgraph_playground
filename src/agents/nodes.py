"""
LangGraphノード定義
各処理ステップを関数として定義
"""

import asyncio
import json
from datetime import date as dt_date

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from src.agents.state import AgentState
from src.external.db.session import execute_sql
from src.external.weather.open_meteo_client import get_weather_on_date
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
"""
        else:
            retry_context = f"""
【前回のエラー - SQL実行エラー】
生成したSQL: {state.get('sql_query', '')}
エラー: {state.get('error', '')}

構文エラーを修正してください。
"""

    # 天気が必要な場合はdateを取得してもらうように指定する。
    date_rule = ""
    if state.get("needs_weather"):
        date_rule = """- 【重要】天気情報と紐付けるため、必ず以下を守ってください：
- SELECT句に `ds.date` を含める
- GROUP BY句にも `ds.date` を含める
- 例: SELECT ds.date, c.name, SUM(ds.clicks) ... GROUP BY ds.date, c.name
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
{date_rule}"""

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
        data = result.get("data", []) or []
        formatted = f"結果: {result['row_count']}件\n{json.dumps(result['data'], ensure_ascii=False, default=str)}"
        return {
            **state,
            "sql_result": formatted,
            "sql_result_data": data,
            "error": None,
            "error_type": None,
        }
    else:
        return {
            **state,
            "sql_result": "",
            "sql_result_data": [],
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
    SQL実行結果を判定し、次のノードを決定

    Returns:
        str: 次のノード名
            - "fetch_weather": 成功 & 天気が必要
            - "generate_answer": 成功 & 天気不要
            - "retry": エラー & リトライ可能
            - "error": エラー & リトライ上限
    """
    if state.get("error"):
        if state.get("retry_count", 0) < settings.max_retries:
            return "retry"
        return "error"

    # 成功時: 天気が必要かどうかで分岐
    if state.get("needs_weather"):
        return "fetch_weather"
    return "generate_answer"


# ここから天気関連のnode
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


def fetch_weather_node(state: AgentState) -> AgentState:
    """
    天気情報を取得するノード（SQL結果から抽出した date に合致する天気を取得）

    前提:
    - state["weather_locations"]: ["tokyo", "osaka"] のようなキー（CITY_COORDINATESのキー）
    - state["weather_dates"]: ["2025-12-01", "2025-12-02"] のような YYYY-MM-DD の配列
    - get_weather_on_date(location, target_date): 指定日天気を返す async 関数
    """
    locations = state.get("weather_locations", []) or []
    dates = state.get("weather_dates", []) or []

    # 非同期処理を同期的に実行（locations × dates を全部取りに行く）
    async def fetch_all():
        tasks = [get_weather_on_date(loc, d) for loc in locations for d in dates]
        return await asyncio.gather(*tasks)

    try:
        results = asyncio.run(fetch_all())

        weather_info = []
        all_success = True
        errors = []

        for result in results:
            # result は get_weather_on_date の戻り値想定
            # 例: {"location": "東京", "date": "2025-12-01", "weather_description": "...", ...}
            weather_info.append(
                {
                    "location": result.get("location"),
                    "date": result.get("date"),
                    "weather_code": result.get("weather_code"),
                    "weather_description": result.get("weather_description"),
                    "temp_max": result.get("temp_max"),
                    "temp_min": result.get("temp_min"),
                }
            )

            if not result.get("success"):
                all_success = False
                if result.get("error"):
                    errors.append(result["error"])

        return {
            **state,
            "weather_info": weather_info,
            "weather_api_history": {
                "called": True,
                "locations": locations,
                "dates": dates,
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
                "dates": dates,
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
            temp_max = info.get("temp_max")
            temp_min = info.get("temp_min")
            if temp_max is not None or temp_min is not None:
                temp_str = f"最高 {temp_max}°C / 最低 {temp_min}°C"
                weather_parts.append(
                    f"{info.get('location')} ({info.get('date')}): {info.get('weather_description')}、{temp_str}"
                )
        if weather_parts:
            weather_text = "\n\n【天気情報】\n" + "\n".join(weather_parts)

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


def extract_weather_dates_node(state: AgentState) -> AgentState:
    """
    SQL実行結果(sql_result_data)から date カラムを抽出して weather_dates に入れる
    """
    rows = state.get("sql_result_data", []) or []
    dates: list[str] = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        d = row.get("date")  # ←ここを基準にする
        if d is None:
            continue

        # DBによって date型 or 文字列があり得るので吸収
        if isinstance(d, dt_date):
            dates.append(d.strftime("%Y-%m-%d"))
        else:
            ds = str(d)[:10]  # "YYYY-MM-DD..." を想定して先頭10
            # 雑に弾く（必要なら厳密化）
            if len(ds) == 10 and ds[4] == "-" and ds[7] == "-":
                dates.append(ds)

    # 重複排除しつつ順序維持
    uniq = list(dict.fromkeys(dates))

    return {**state, "weather_dates": uniq}
