"""
Evidence Builder (Ads-ops minimum strong, safer drop-in)
- 派生指標追加（CTR/CPC/CVR/CPA/ROAS）
- dimension検出改善（DDL前提の安定ルール）
- 期間比較（dateが“日別集計っぽい”ときだけ）
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

# ================== Data Model ==================


@dataclass
class Evidence:
    """構造化されたエビデンス（互換重視で period_comparison を追加）"""

    question: str
    sql: str
    row_count: int

    raw_data: list[dict] = field(default_factory=list)

    aggregations: dict[str, dict[str, float]] = field(default_factory=dict)
    analysis: list[str] = field(default_factory=list)
    rankings: list[dict] = field(default_factory=list)
    category_analysis: dict = field(default_factory=dict)
    share_analysis: dict = field(default_factory=dict)

    # 追加：期間比較（存在しなくても良い/空dict）
    period_comparison: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "question": self.question,
            "sql": self.sql,
            "row_count": self.row_count,
            "aggregations": self.aggregations,
            "analysis": self.analysis,
            "rankings": self.rankings,
            "share_analysis": self.share_analysis,
            "category_analysis": self.category_analysis,
            "period_comparison": self.period_comparison,
        }

    def to_prompt(self) -> str:
        """Evidenceをプロンプト用の文字列に変換する"""
        parts = [
            f"## 質問\n{self.question}",
            f"\n## 実行SQL\n\n{self.sql}\n```",
            f"\n## 結果行数: {self.row_count}件",
        ]

        if self.aggregations:
            parts.append("\n## 集計結果")
            for metric, values in self.aggregations.items():
                vals_str = ", ".join(f"{k}: {v:.2f}" for k, v in values.items())
                parts.append(f"- {metric}: {vals_str}")

        if self.analysis:
            parts.append("\n## 分析")
            for a in self.analysis:
                parts.append(f"- {a}")

        if self.rankings:
            parts.append("\n## ランキング")
            for r in self.rankings:
                parts.append(f"- {r['rank']}位: {r['name']} ({r['metric']}: {r['value']:.2f})")

        if self.share_analysis:
            parts.append("\n## シェア分析")
            parts.append(
                f"- トップ: {self.share_analysis.get('top_name')} ({self.share_analysis.get('top_share', 0):.1f}%)"
            )
            if "top3_share" in self.share_analysis:
                parts.append(f"- 上位3件合計: {self.share_analysis['top3_share']:.1f}%")

        if self.category_analysis:
            parts.append("\n## カテゴリ分析")
            if "best" in self.category_analysis:
                parts.append(
                    f"- ベスト: {self.category_analysis['best']['name']} (平均: {self.category_analysis['best']['avg']:.2f})"
                )
            if "worst" in self.category_analysis:
                parts.append(
                    f"- ワースト: {self.category_analysis['worst']['name']} (平均: {self.category_analysis['worst']['avg']:.2f})"
                )

        if self.period_comparison:
            parts.append("\n## 期間比較")
            if "summary" in self.period_comparison:
                parts.append(f"- {self.period_comparison['summary']}")

        return "\n".join(parts)


# ================== Helpers ==================


def _parse_sql_result(sql_result: str) -> list[dict]:
    """SQL結果（JSON文字列）をパース。ログ混在にも耐える。"""
    try:
        match = re.search(r"\[.*\]", sql_result, re.DOTALL)
        if match:
            loaded = json.loads(match.group())
            return loaded if isinstance(loaded, list) else []
    except json.JSONDecodeError:
        pass
    return []


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def _is_date_like(x: Any) -> bool:
    if x is None or not isinstance(x, str):
        return False
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", x.strip()))


def _get_label(col: str) -> str:
    col_lower = col.lower()
    labels = {
        "impressions": "表示回数",
        "clicks": "クリック数",
        "cost": "費用",
        "conversions": "CV数",
        "conversion_value": "CV価値",
        "ctr": "CTR",
        "cpc": "CPC",
        "cvr": "CVR",
        "cpa": "CPA",
        "roas": "ROAS",
        "date": "日付",
    }
    return labels.get(col_lower, col.replace("_", " ").title())


# ================== 1) Derived metrics ==================


def _add_derived_metrics(data: list[dict]) -> list[dict]:
    """
    impressions, clicks, cost, conversions, conversion_value
    をベースに派生指標を行ごとに追加。
    """
    out: list[dict] = []
    for row in data:
        r = dict(row)

        imp = _safe_float(r.get("impressions"))
        clk = _safe_float(r.get("clicks"))
        cost = _safe_float(r.get("cost"))
        conv = _safe_float(r.get("conversions"))
        convv = _safe_float(r.get("conversion_value"))

        # CTR = clicks / impressions
        if imp and imp > 0 and clk is not None:
            r["ctr"] = clk / imp

        # CPC = cost / clicks
        if clk and clk > 0 and cost is not None:
            r["cpc"] = cost / clk

        # CVR = conversions / clicks
        if clk and clk > 0 and conv is not None:
            r["cvr"] = conv / clk

        # CPA = cost / conversions
        if conv and conv > 0 and cost is not None:
            r["cpa"] = cost / conv

        # ROAS = conversion_value / cost
        if cost and cost > 0 and convv is not None:
            r["roas"] = convv / cost

        out.append(r)
    return out


# ================== 2) Dimension detection (DDL-aware) ==================


DIMENSION_PRIORITY = [
    "campaign_name",
    "ad_group_name",
    "keyword_text",
    "query_text",
    "service_name",
    "campaign_type",
    "match_type",
    "status",
    "targeting_type",
    "targeting_value",
    "date",  # dateは最後寄り（ランキングが日付になるのを避けたい）
    "name",
]


def _detect_dimension_and_metrics(row: dict) -> tuple[Optional[str], list[str]]:
    metrics: list[str] = []
    non_numeric_cols: list[str] = []

    for col, val in row.items():
        cl = col.lower()

        # ID/GoogleIDは除外（表示軸になりにくい）
        if cl == "id" or cl.endswith("_id") or cl.startswith("google_"):
            continue

        # dateは次元候補
        if cl == "date" or _is_date_like(val):
            non_numeric_cols.append(col)
            continue

        f = _safe_float(val)
        if f is not None:
            metrics.append(col)
        else:
            non_numeric_cols.append(col)

    # dimensionを優先順位で決定
    lowers = {c.lower(): c for c in row.keys()}
    dimension: Optional[str] = None
    for key in DIMENSION_PRIORITY:
        if key in lowers:
            dimension = lowers[key]
            break

    if dimension is None:
        dimension = non_numeric_cols[0] if non_numeric_cols else None

    # metricsの並び順を安定化（重要指標優先）
    priority = [
        "cpa",
        "roas",
        "cpc",
        "cvr",
        "ctr",
        "cost",
        "conversions",
        "clicks",
        "impressions",
        "conversion_value",
    ]

    def metric_priority(col: str) -> int:
        c = col.lower()
        for i, p in enumerate(priority):
            if c == p or p in c:
                return i
        return 100

    metrics = sorted(metrics, key=metric_priority)
    return dimension, metrics


# ================== 3) Aggregations / Analysis / Ranking ==================


def _calculate_aggregations(
    data: list[dict], metric_cols: list[str]
) -> dict[str, dict[str, float]]:
    """
    注意：派生指標（CTR/CPC/CVR/CPA/ROAS）は「合計」を出すと誤解を生むので、
    派生指標は平均/最大/最小だけにする。
    """
    result: dict[str, dict[str, float]] = {}
    derived = {"ctr", "cpc", "cvr", "cpa", "roas"}

    for col in metric_cols[:7]:
        values: list[float] = []
        for row in data:
            v = _safe_float(row.get(col))
            if v is not None:
                values.append(v)

        if not values:
            continue

        label = _get_label(col)
        if col.lower() in derived:
            result[label] = {
                "平均": sum(values) / len(values),
                "最大": max(values),
                "最小": min(values),
            }
        else:
            result[label] = {
                "合計": sum(values),
                "平均": sum(values) / len(values),
                "最大": max(values),
                "最小": min(values),
            }

    return result


def _generate_analysis(data: list[dict], metric_cols: list[str]) -> list[str]:
    analysis: list[str] = []
    for col in metric_cols[:4]:
        values = []
        for row in data:
            v = _safe_float(row.get(col))
            if v is not None:
                values.append(v)

        if len(values) < 2:
            continue

        label = _get_label(col)
        mx, mn = max(values), min(values)
        avg = sum(values) / len(values)

        if mn > 0:
            analysis.append(f"{label}の最大は最小の{(mx / mn):.1f}倍")

        if avg > 0:
            var = sum((v - avg) ** 2 for v in values) / len(values)
            std = var**0.5
            cv = (std / avg) * 100
            if cv > 50:
                disp = "非常に大きい"
            elif cv > 30:
                disp = "大きい"
            elif cv > 15:
                disp = "中程度"
            else:
                disp = "小さい"
            analysis.append(f"{label}のばらつきは{disp}（変動係数{cv:.1f}%）")

    return analysis


def _generate_rankings(data: list[dict], dimension: Optional[str], metric: str) -> list[dict]:
    if not dimension:
        return []

    valid = []
    for row in data:
        v = _safe_float(row.get(metric))
        if v is None:
            continue
        valid.append({"name": str(row.get(dimension, "不明")), "value": v})

    valid.sort(key=lambda x: x["value"], reverse=True)
    metric_label = _get_label(metric)
    return [
        {"rank": i + 1, "name": r["name"], "metric": metric_label, "value": r["value"]}
        for i, r in enumerate(valid[:5])
    ]


def _calculate_share_analysis(data: list[dict], dimension: Optional[str], metric: str) -> dict:
    if not dimension:
        return {}

    pairs = []
    for row in data:
        v = _safe_float(row.get(metric))
        if v is None:
            continue
        pairs.append((str(row.get(dimension, "不明")), v))

    if not pairs:
        return {}
    total = sum(v for _, v in pairs)
    if total <= 0:
        return {}

    pairs.sort(key=lambda x: x[1], reverse=True)
    top_name, top_val = pairs[0]
    out = {"top_name": top_name, "top_share": (top_val / total) * 100}
    if len(pairs) >= 3:
        top3 = sum(v for _, v in pairs[:3])
        out["top3_share"] = (top3 / total) * 100
    return out


def _calculate_category_analysis(data: list[dict], dimension: Optional[str], metric: str) -> dict:
    if not dimension:
        return {}

    cat_vals: dict[str, list[float]] = {}
    for row in data:
        v = _safe_float(row.get(metric))
        if v is None:
            continue
        name = str(row.get(dimension, ""))
        cat = name.split("_")[0] if "_" in name else name
        if not cat:
            continue
        cat_vals.setdefault(cat, []).append(v)

    if len(cat_vals) < 2:
        return {}

    avgs = [(cat, sum(vals) / len(vals)) for cat, vals in cat_vals.items()]
    avgs.sort(key=lambda x: x[1])

    best_cat, best_avg = avgs[0]
    worst_cat, worst_avg = avgs[-1]

    ml = metric.lower()
    lower_is_better = any(x in ml for x in ["cpa", "cpc", "cost"])
    if lower_is_better:
        return {
            "best": {"name": best_cat, "avg": best_avg},
            "worst": {
                "name": worst_cat,
                "avg": worst_avg,
                "ratio": (worst_avg / best_avg) if best_avg > 0 else 0,
            },
        }
    else:
        return {
            "best": {"name": worst_cat, "avg": worst_avg},
            "worst": {
                "name": best_cat,
                "avg": best_avg,
                "ratio": (worst_avg / best_avg) if best_avg > 0 else 0,
            },
        }


# ================== 4) Period comparison (only when daily-like) ==================


def _calculate_period_comparison(data: list[dict], metric_cols: list[str]) -> dict:
    # date列を探す
    date_col = None
    if data and "date" in data[0]:
        date_col = "date"
    else:
        # 1行目から日付っぽい列を探す
        for c, v in (data[0].items() if data else []):
            if c.lower() == "date" or _is_date_like(v):
                date_col = c
                break

    if not date_col:
        return {}

    # 日付っぽい行だけ抽出してソート
    rows = [r for r in data if _is_date_like(r.get(date_col))]
    if len(rows) < 4:
        return {}

    rows.sort(key=lambda r: str(r.get(date_col)))
    unique_dates = len({r.get(date_col) for r in rows})

    # ★日別集計っぽいか簡易判定：
    # 行数に対してユニーク日付が十分多い（= dateが主要な軸）
    if unique_dates < max(3, len(rows) // 3):
        return {}

    mid = len(rows) // 2
    prev, curr = rows[:mid], rows[mid:]
    if not prev or not curr:
        return {}

    def sum_metric(rs: list[dict], col: str) -> float:
        s = 0.0
        for r in rs:
            v = _safe_float(r.get(col))
            if v is not None:
                s += v
        return s

    target_cols = metric_cols[:5]
    out = {
        "date_col": date_col,
        "prev_range": {"start": prev[0][date_col], "end": prev[-1][date_col]},
        "curr_range": {"start": curr[0][date_col], "end": curr[-1][date_col]},
        "metrics": {},
    }

    for col in target_cols:
        prev_sum = sum_metric(prev, col)
        curr_sum = sum_metric(curr, col)
        change = curr_sum - prev_sum
        change_pct = ((change) / abs(prev_sum) * 100) if prev_sum != 0 else None
        out["metrics"][_get_label(col)] = {
            "prev": prev_sum,
            "curr": curr_sum,
            "change": change,
            "change_pct": change_pct,
        }

    # ざっくり要約（あるものだけ）
    summary = []
    for k in ["費用", "CV数", "ROAS", "CPA", "クリック数", "表示回数"]:
        m = out["metrics"].get(k)
        if m and m["change_pct"] is not None:
            summary.append(f"{k}は{m['change_pct']:+.1f}%")
    if summary:
        out["summary"] = "、".join(summary)

    return out


# ================== Main ==================


def build_evidence(sql_result: str, sql_query: str, question: str) -> Evidence:
    data = _parse_sql_result(sql_result)
    if not data:
        return Evidence(question=question, sql=sql_query, row_count=0)

    # 1) 派生指標追加
    data = _add_derived_metrics(data)

    # 2) dimension/metrics検出
    dimension, metrics = _detect_dimension_and_metrics(data[0])

    aggregations = {}
    analysis = []
    rankings = []
    share_analysis = {}
    category_analysis = {}
    period_comparison = {}

    # 3) 集計（metricsがあれば）
    if metrics:
        aggregations = _calculate_aggregations(data, metrics)

    # 4) ばらつき等（2行以上推奨）
    if len(data) >= 2 and metrics:
        analysis = _generate_analysis(data, metrics)

    # 5) dimension必須の分析
    if dimension and metrics:
        rankings = _generate_rankings(data, dimension, metrics[0])
        share_analysis = _calculate_share_analysis(data, dimension, metrics[0])
        category_analysis = _calculate_category_analysis(data, dimension, metrics[0])

    # 6) 期間比較（日別っぽいときだけ）
    if metrics:
        period_comparison = _calculate_period_comparison(data, metrics)

    return Evidence(
        question=question,
        sql=sql_query,
        row_count=len(data),
        raw_data=data,
        aggregations=aggregations,
        analysis=analysis,
        rankings=rankings,
        share_analysis=share_analysis,
        category_analysis=category_analysis,
        period_comparison=period_comparison,
    )
