"""
Open-Meteo API クライアント
無料の天気APIを使用して天気情報を取得
"""

import httpx

# 都市の緯度経度
CITY_COORDINATES = {
    "tokyo": {"latitude": 35.6762, "longitude": 139.6503, "name": "東京"},
    "osaka": {"latitude": 34.6937, "longitude": 135.5023, "name": "大阪"},
}

# WMO天気コードの説明
WEATHER_CODES = {
    0: "快晴",
    1: "晴れ",
    2: "一部曇り",
    3: "曇り",
    45: "霧",
    48: "着氷性の霧",
    51: "弱い霧雨",
    53: "霧雨",
    55: "強い霧雨",
    61: "弱い雨",
    63: "雨",
    65: "強い雨",
    71: "弱い雪",
    73: "雪",
    75: "強い雪",
    80: "弱いにわか雨",
    81: "にわか雨",
    82: "激しいにわか雨",
    95: "雷雨",
    96: "雹を伴う雷雨",
    99: "激しい雹を伴う雷雨",
}


async def get_weather(location: str) -> dict:
    """
    指定した場所の現在の天気を取得

    Args:
        location: 場所（"tokyo" or "osaka"）

    Returns:
        dict: 天気情報
            - location: 場所
            - temperature: 気温
            - weather_code: 天気コード
            - weather_description: 天気の説明
            - success: 成功したかどうか
            - error: エラーメッセージ（あれば）
    """
    if location not in CITY_COORDINATES:
        return {
            "location": location,
            "temperature": None,
            "weather_code": None,
            "weather_description": "",
            "success": False,
            "error": f"未対応の場所: {location}",
        }

    city = CITY_COORDINATES[location]
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current_weather": True,
        "timezone": "Asia/Tokyo",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

        current = data.get("current_weather", {})
        weather_code = current.get("weathercode", 0)

        return {
            "location": city["name"],
            "temperature": current.get("temperature"),
            "weather_code": weather_code,
            "weather_description": WEATHER_CODES.get(weather_code, "不明"),
            "success": True,
            "error": None,
        }
    except httpx.HTTPStatusError as e:
        return {
            "location": city["name"],
            "temperature": None,
            "weather_code": None,
            "weather_description": "",
            "success": False,
            "error": f"HTTP Error: {e.response.status_code}",
        }
    except Exception as e:
        return {
            "location": city["name"],
            "temperature": None,
            "weather_code": None,
            "weather_description": "",
            "success": False,
            "error": str(e),
        }
