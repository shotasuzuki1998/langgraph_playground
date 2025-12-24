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


async def get_weather_on_date(location: str, target_date: str) -> dict:
    """
    指定した場所・日付(YYYY-MM-DD)の天気を取得（daily）
    """
    if location not in CITY_COORDINATES:
        return {
            "location": location,
            "date": target_date,
            "weather_code": None,
            "weather_description": "",
            "temp_max": None,
            "temp_min": None,
            "success": False,
            "error": f"未対応の場所: {location}",
        }

    city = CITY_COORDINATES[location]
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": target_date,
        "end_date": target_date,
        "daily": "weather_code,temperature_2m_max,temperature_2m_min",
        "timezone": "Asia/Tokyo",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        daily = data.get("daily") or {}
        # 1日分の配列が返る想定
        codes = daily.get("weather_code") or []
        tmax = daily.get("temperature_2m_max") or []
        tmin = daily.get("temperature_2m_min") or []

        if not codes:
            return {
                "location": city["name"],
                "date": target_date,
                "weather_code": None,
                "weather_description": "",
                "temp_max": None,
                "temp_min": None,
                "success": False,
                "error": "daily weather_code が取得できませんでした",
            }

        wc = int(codes[0])
        return {
            "location": city["name"],
            "date": target_date,
            "weather_code": wc,
            "weather_description": WEATHER_CODES.get(wc, "不明"),
            "temp_max": tmax[0] if tmax else None,
            "temp_min": tmin[0] if tmin else None,
            "success": True,
            "error": None,
        }

    except httpx.HTTPStatusError as e:
        return {
            "location": city["name"],
            "date": target_date,
            "weather_code": None,
            "weather_description": "",
            "temp_max": None,
            "temp_min": None,
            "success": False,
            "error": f"HTTP Error: {e.response.status_code}",
        }
    except Exception as e:
        return {
            "location": city["name"],
            "date": target_date,
            "weather_code": None,
            "weather_description": "",
            "temp_max": None,
            "temp_min": None,
            "success": False,
            "error": str(e),
        }
