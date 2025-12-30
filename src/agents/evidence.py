"""
Evidence Builder
SQL結果を構造化されたエビデンスに変換
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Evidence:
    """構造化されたエビデンス"""

    question: str
    sql: str
    row_count: int

    # データ
    raw_data: list[dict] = field(default_factory=list)

    # 集計値
    # 例: {"費用": {"合計": 200000000, "平均": 66000000, ...}}
    aggregations: dict[str, dict[str, float]] = field(default_factory=dict)

    # 分析結果
    # 例: ["最大は最小の4.3倍", "ばらつきは大きい（変動係数53%）"]
    analysis: list[str] = field(default_factory=list)

    # ランキング
    # 例: [{"rank": 1, "name": "転職C", "metric": "費用", "value": 92000000}, ...]
    rankings: list[dict] = field(default_factory=list)

    # カテゴリ分析
    # 例: {"best": {"name": "ECサイト", "avg": 1000}, "worst": {...}}
    category_analysis: dict = field(default_factory=dict)

    # シェア分析
    # 例: {"top_name": "転職C", "top_share": 46.1, "top3_share": 100.0}
    share_analysis: dict = field(default_factory=dict)

    def to_prompt(self) -> str:
        """LLMに渡すプロンプト形式に変換"""
        lines = [
            f"## 質問\n{self.question}\n",
            f"## 実行したSQL\n```sql\n{self.sql}\n```\n",
            f"## データ件数: {self.row_count}件\n",
        ]

        # 集計値
        if self.aggregations:
            lines.append("### 📊 集計値")
            for metric, values in self.aggregations.items():
                for agg_type, value in values.items():
                    lines.append(f"- {metric}の{agg_type}: {_format_number(value)}")
            lines.append("")

        # 分析
        if self.analysis:
            lines.append("### 📈 分析")
            for item in self.analysis:
                lines.append(f"- {item}")
            lines.append("")

        # シェア分析
        if self.share_analysis:
            lines.append("### 📊 シェア分析")
            if "top_name" in self.share_analysis:
                lines.append(
                    f"- トップの「{self.share_analysis['top_name']}」が"
                    f"全体の{self.share_analysis['top_share']:.1f}%を占める"
                )
            if "top3_share" in self.share_analysis:
                lines.append(f"- 上位3件で全体の{self.share_analysis['top3_share']:.1f}%を占める")
            lines.append("")

        # カテゴリ分析
        if self.category_analysis:
            lines.append("### 🏷️ カテゴリ分析")
            if "best" in self.category_analysis:
                best = self.category_analysis["best"]
                lines.append(
                    f"- 最も効率的: 「{best['name']}」系（平均={_format_number(best['avg'])}）"
                )
            if "worst" in self.category_analysis:
                worst = self.category_analysis["worst"]
                ratio = worst.get("ratio", 0)
                lines.append(
                    f"- 改善余地あり: 「{worst['name']}」系"
                    f"（平均={_format_number(worst['avg'])}、{ratio:.1f}倍）"
                )
            lines.append("")

        # ランキング
        if self.rankings:
            lines.append("### 🏆 ランキング")
            for r in self.rankings:
                lines.append(
                    f"- 第{r['rank']}位: {r['name']}（{r['metric']}={_format_number(r['value'])}）"
                )
            lines.append("")

        # 生データ（先頭5件）
        if self.raw_data:
            lines.append("### 📋 データ（先頭5件）")
            for i, row in enumerate(self.raw_data[:5]):
                formatted_row = ", ".join(f"{k}={_format_value(v)}" for k, v in row.items())
                lines.append(f"- {formatted_row}")
            lines.append("")

        return "\n".join(lines)

    def to_dict(self) -> dict:
        """辞書形式に変換（デバッグ用）"""
        return {
            "question": self.question,
            "sql": self.sql,
            "row_count": self.row_count,
            "aggregations": self.aggregations,
            "analysis": self.analysis,
            "rankings": self.rankings,
            "share_analysis": self.share_analysis,
            "category_analysis": self.category_analysis,
        }


# ================== ヘルパー関数 ==================


def _format_number(value: float) -> str:
    """数値を読みやすくフォーマット"""
    if value is None:
        return "N/A"
    try:
        value = float(value)
        if abs(value) >= 1_000_000:
            return f"{value/1_000_000:,.2f}M"
        elif abs(value) >= 1_000:
            return f"{value/1_000:,.2f}K"
        elif abs(value) < 1 and value != 0:
            return f"{value:.4f}"
        return f"{value:,.2f}"
    except (ValueError, TypeError):
        return str(value)


def _format_value(value: Any) -> str:
    """値をフォーマット（数値以外も対応）"""
    if value is None:
        return "N/A"
    try:
        num = float(value)
        return _format_number(num)
    except (ValueError, TypeError):
        return str(value)


def _parse_sql_result(sql_result: str) -> list[dict]:
    """SQL結果をパース"""
    try:
        match = re.search(r"\[.*\]", sql_result, re.DOTALL)
        if match:
            return json.loads(match.group())
    except json.JSONDecodeError:
        pass
    return []


def _get_label(col: str) -> str:
    """カラム名から日本語ラベルを取得"""
    col_lower = col.lower()

    # 基本指標
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
    }

    if col_lower in labels:
        return labels[col_lower]

    # パターンマッチング
    patterns = {
        "impression": "表示回数",
        "click": "クリック数",
        "cost": "費用",
        "conversion": "CV数",
        "spend": "費用",
        "cpa": "CPA",
        "cpc": "CPC",
        "ctr": "CTR",
        "cvr": "CVR",
        "roas": "ROAS",
    }

    prefixes = {
        "total": "総",
        "sum": "合計",
        "avg": "平均",
        "average": "平均",
        "max": "最大",
        "min": "最小",
    }

    # プレフィックス検出
    prefix_label = ""
    remaining = col_lower
    for prefix, jp_prefix in prefixes.items():
        if remaining.startswith(prefix + "_"):
            prefix_label = jp_prefix
            remaining = remaining[len(prefix) + 1 :]
            break

    # パターンマッチング
    for pattern, label in sorted(patterns.items(), key=lambda x: -len(x[0])):
        if pattern in remaining:
            return f"{prefix_label}{label}" if prefix_label else label

    # フォールバック
    if prefix_label:
        return f"{prefix_label}{remaining.replace('_', ' ').title()}"
    return col.replace("_", " ").title()


def _detect_columns(row: dict) -> tuple[str | None, list[str]]:
    """ディメンション列とメトリクス列を検出"""
    dimension = None
    metrics = []

    for col, val in row.items():
        col_lower = col.lower()

        # IDカラムは除外
        if col_lower.endswith("_id") or col_lower == "id":
            continue

        # nameを含むカラムはディメンション
        if "name" in col_lower:
            dimension = col
            continue

        # 数値型はメトリクス
        if val is not None:
            try:
                float(val)
                metrics.append(col)
            except (ValueError, TypeError):
                # 文字列型でディメンションが未設定なら設定
                if dimension is None:
                    dimension = col

    # 重要な指標を先にソート
    priority = ["cpa", "roas", "cpc", "cvr", "ctr", "cost", "conversions", "clicks", "impressions"]

    def metric_priority(col: str) -> int:
        col_lower = col.lower()
        for i, p in enumerate(priority):
            if p in col_lower:
                return i
        return 100

    metrics = sorted(metrics, key=metric_priority)

    return dimension, metrics


def _calculate_aggregations(
    data: list[dict], metric_cols: list[str]
) -> dict[str, dict[str, float]]:
    """集計値を計算"""
    result = {}

    for col in metric_cols[:5]:  # 上位5指標
        values = []
        for row in data:
            if col in row and row[col] is not None:
                try:
                    values.append(float(row[col]))
                except (ValueError, TypeError):
                    pass

        if values:
            label = _get_label(col)
            result[label] = {
                "合計": sum(values),
                "平均": sum(values) / len(values),
                "最大": max(values),
                "最小": min(values),
            }

    return result


def _generate_analysis(data: list[dict], metric_cols: list[str]) -> list[str]:
    """分析コメントを生成"""
    analysis = []

    for col in metric_cols[:3]:  # 上位3指標
        values = []
        for row in data:
            if col in row and row[col] is not None:
                try:
                    values.append(float(row[col]))
                except (ValueError, TypeError):
                    pass

        if not values:
            continue

        label = _get_label(col)
        max_val = max(values)
        min_val = min(values)
        avg_val = sum(values) / len(values)

        # 比率分析
        if min_val > 0:
            ratio = max_val / min_val
            analysis.append(f"{label}の最大は最小の{ratio:.1f}倍")

        # ばらつき分析（変動係数）
        if len(values) >= 2 and avg_val > 0:
            variance = sum((v - avg_val) ** 2 for v in values) / len(values)
            std_dev = variance**0.5
            cv = (std_dev / avg_val) * 100

            if cv > 50:
                dispersion = "非常に大きい"
            elif cv > 30:
                dispersion = "大きい"
            elif cv > 15:
                dispersion = "中程度"
            else:
                dispersion = "小さい"

            analysis.append(f"{label}のばらつきは{dispersion}（変動係数{cv:.1f}%）")

    return analysis


def _generate_rankings(data: list[dict], dimension: str | None, metric: str) -> list[dict]:
    """ランキングを生成"""
    if not dimension:
        return []

    valid_data = []
    for row in data:
        if metric in row and row[metric] is not None:
            try:
                valid_data.append({"name": row.get(dimension, "不明"), "value": float(row[metric])})
            except (ValueError, TypeError):
                pass

    sorted_data = sorted(valid_data, key=lambda x: x["value"], reverse=True)
    metric_label = _get_label(metric)

    return [
        {"rank": i + 1, "name": d["name"], "metric": metric_label, "value": d["value"]}
        for i, d in enumerate(sorted_data[:5])
    ]


def _calculate_share_analysis(data: list[dict], dimension: str | None, metric: str) -> dict:
    """シェア分析を計算"""
    if not dimension:
        return {}

    values_with_names = []
    for row in data:
        if metric in row and row[metric] is not None:
            try:
                val = float(row[metric])
                name = row.get(dimension, "不明")
                values_with_names.append((name, val))
            except (ValueError, TypeError):
                pass

    if not values_with_names:
        return {}

    total = sum(v for _, v in values_with_names)
    if total <= 0:
        return {}

    sorted_values = sorted(values_with_names, key=lambda x: x[1], reverse=True)
    top_name, top_value = sorted_values[0]
    top_share = (top_value / total) * 100

    result = {"top_name": top_name, "top_share": top_share}

    if len(sorted_values) >= 3:
        top3_total = sum(v for _, v in sorted_values[:3])
        result["top3_share"] = (top3_total / total) * 100

    return result


def _calculate_category_analysis(data: list[dict], dimension: str | None, metric: str) -> dict:
    """カテゴリ別分析を計算"""
    if not dimension:
        return {}

    # 名前からカテゴリを抽出
    category_values: dict[str, list[float]] = {}
    for row in data:
        if metric in row and row[metric] is not None:
            try:
                val = float(row[metric])
                name = str(row.get(dimension, ""))
                category = name.split("_")[0] if "_" in name else name
                if category:
                    if category not in category_values:
                        category_values[category] = []
                    category_values[category].append(val)
            except (ValueError, TypeError):
                pass

    if len(category_values) < 2:
        return {}

    # カテゴリ別の平均を計算
    category_avgs = [
        (cat, sum(vals) / len(vals), len(vals)) for cat, vals in category_values.items()
    ]
    category_avgs.sort(key=lambda x: x[1])

    best_cat, best_avg, _ = category_avgs[0]
    worst_cat, worst_avg, _ = category_avgs[-1]

    # CPAなどは低い方が良い
    metric_lower = metric.lower()
    if any(x in metric_lower for x in ["cpa", "cpc", "cost"]):
        return {
            "best": {"name": best_cat, "avg": best_avg},
            "worst": {
                "name": worst_cat,
                "avg": worst_avg,
                "ratio": worst_avg / best_avg if best_avg > 0 else 0,
            },
        }
    else:
        # CV数などは高い方が良い
        return {
            "best": {"name": worst_cat, "avg": worst_avg},
            "worst": {
                "name": best_cat,
                "avg": best_avg,
                "ratio": worst_avg / best_avg if best_avg > 0 else 0,
            },
        }


# ================== メイン関数 ==================


def build_evidence(sql_result: str, sql_query: str, question: str) -> Evidence:
    """
    SQL結果からEvidenceを構築

    Args:
        sql_result: SQL実行結果（JSON文字列）
        sql_query: 実行したSQL
        question: 元の質問

    Returns:
        Evidence: 構造化されたエビデンス
    """
    data = _parse_sql_result(sql_result)

    if not data:
        return Evidence(question=question, sql=sql_query, row_count=0)

    dimension, metrics = _detect_columns(data[0])

    # 各種分析を実行
    aggregations = {}
    analysis = []
    rankings = []
    share_analysis = {}
    category_analysis = {}

    if len(data) >= 1 and metrics:
        aggregations = _calculate_aggregations(data, metrics)

    if len(data) >= 2 and metrics:
        analysis = _generate_analysis(data, metrics)
        share_analysis = _calculate_share_analysis(data, dimension, metrics[0])
        category_analysis = _calculate_category_analysis(data, dimension, metrics[0])

    if dimension and metrics:
        rankings = _generate_rankings(data, dimension, metrics[0])

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
    )
