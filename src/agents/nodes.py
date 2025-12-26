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
    """
    SQLを生成するノード
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
"""
        else:
            retry_context = f"""
【前回のエラー - SQL実行エラー】
生成したSQL: {state.get('sql_query', '')}
エラー: {state.get('error', '')}

構文エラーを修正してください。
"""
    # 天気情報の取得はSQL結果に日付があるかどうかで判断する

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
        "weather_unavailable": False,  # 初期化
        "weather_unavailable_reason": None,  # 初期化
        "weather_api_history": {
            "called": False,
            "locations": [],
            "success": False,
            "error": None,
        },
    }


def extract_weather_dates_node(state: AgentState) -> AgentState:
    """
    SQL実行結果(sql_result_data)から date カラムを抽出して weather_dates に入れる
    日付が取得できない場合は weather_unavailable を True に設定
    """
    # 天気が不要な場合はスキップ
    if not state.get("needs_weather"):
        return state

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

    # 日付が抽出できなかった場合
    if not uniq:
        return {
            **state,
            "weather_dates": [],
            "weather_unavailable": True,
            "weather_unavailable_reason": (
                "SQL結果に日付情報が含まれていないため、天気情報を取得できませんでした。\n"
                "日付を特定できる質問（例：「〇〇が最も良かった日は？」「先週の日別データ」など）をお試しください。"
            ),
        }

    # 日付が多すぎる場合は制限（429エラー防止）
    MAX_DATES = 10
    weather_note = None
    if len(uniq) > MAX_DATES:
        uniq = uniq[:MAX_DATES]
        weather_note = f"※天気情報は最新{MAX_DATES}日分のみ表示しています"

    return {
        **state,
        "weather_dates": uniq,
        "weather_unavailable": False,
        "weather_note": weather_note,
    }


def fetch_weather_node(state: AgentState) -> AgentState:
    """
    天気情報を取得するノード（SQL結果から抽出した date に合致する天気を取得）
    日付が取得できなかった場合はスキップ

    前提:
    - state["weather_locations"]: ["tokyo", "osaka"] のようなキー（CITY_COORDINATESのキー）
    - state["weather_dates"]: ["2025-12-01", "2025-12-02"] のような YYYY-MM-DD の配列
    - get_weather_on_date(location, target_date): 指定日天気を返す async 関数
    """
    # 天気が不要、または日付が取得できなかった場合はスキップ
    if not state.get("needs_weather") or state.get("weather_unavailable"):
        return state

    locations = state.get("weather_locations", []) or []
    dates = state.get("weather_dates", []) or []

    # 日付がない場合はスキップ
    if not dates:
        return {
            **state,
            "weather_unavailable": True,
            "weather_unavailable_reason": "天気取得に必要な日付情報がありません",
        }

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
    天気が取得できなかった場合はその理由も表示

    Args:
        state: 現在のエージェント状態

    Returns:
        AgentState: 更新された状態（answerが設定される）
    """
    # ★ 天気情報の状態を整理
    weather_text = ""

    if state.get("weather_unavailable"):
        # 天気が取得できなかった場合
        reason = state.get("weather_unavailable_reason", "天気情報を取得できませんでした")
        weather_text = f"\n\n【天気情報について】\n{reason}"

    elif state.get("weather_info"):
        # 天気が取得できた場合
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

            # ★ 日付制限の注記があれば追加
            if state.get("weather_note"):
                weather_text += f"\n{state['weather_note']}"

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
天気情報が取得できなかった場合は、その旨を回答の最後に簡潔に記載してください。
"""

    llm = get_llm()
    response = llm.invoke(
        [
            SystemMessage(content="あなたはGoogle広告のデータアナリストです。"),
            HumanMessage(content=prompt),
        ]
    )

    return {**state, "answer": response.content}
