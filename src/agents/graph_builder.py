"""
SQL結果からEvidence Graphを構築
"""

import json
import re
from typing import Any

from src.agents.evidence import (
    EvidenceEdge,
    EvidenceGraph,
    EvidenceNode,
    EvidenceType,
    RelationType,
)


class SQLResultGraphBuilder:
    """SQL結果をEvidence Graphに変換"""

    # 指標の日本語マッピング（拡充版）
    METRIC_LABELS = {
        # 基本指標
        "impressions": "表示回数",
        "clicks": "クリック数",
        "cost": "費用",
        "conversions": "CV数",
        "conversion_value": "CV価値",
        # 計算指標
        "ctr": "CTR",
        "cpc": "CPC",
        "cvr": "CVR",
        "cpa": "CPA",
        "roas": "ROAS",
        # 集計カラム名（SQLで生成されるもの）
        "total_cost": "総費用",
        "total_clicks": "総クリック数",
        "total_impressions": "総表示回数",
        "total_conversions": "総CV数",
        "total_conversion_value": "総CV価値",
        "sum_cost": "費用合計",
        "sum_clicks": "クリック合計",
        "sum_conversions": "CV合計",
        "avg_cpc": "平均CPC",
        "avg_cpa": "平均CPA",
        "avg_cost": "平均費用",
        # IDカラム
        "service_id": "サービスID",
        "account_id": "アカウントID",
        "campaign_id": "キャンペーンID",
        "ad_group_id": "広告グループID",
        "keyword_id": "キーワードID",
        "ad_id": "広告ID",
    }

    # ディメンション列のヒント（拡充版）
    DIMENSION_HINTS = [
        "name",
        "service_name",
        "campaign_name",
        "account_name",
        "ad_group_name",
        "keyword_text",
        "query_text",
        "date",
        "campaign",
        "service",
        "ad_group",
        "ad_type",
        "match_type",
        "status",
        "campaign_type",
    ]

    def __init__(self):
        self.graph = EvidenceGraph()

    def build(self, sql_result: str, sql_query: str, question: str) -> EvidenceGraph:
        """SQL結果からEvidence Graphを構築"""
        self.graph.metadata = {"question": question, "sql": sql_query}

        data = self._parse_sql_result(sql_result)
        if not data:
            return self.graph

        self.graph.metadata["row_count"] = len(data)

        dimension_col = self._detect_dimension_column(data[0])
        metric_cols = self._detect_metric_columns(data[0])

        self._add_data_nodes(data, dimension_col, metric_cols)

        if len(data) >= 2:
            self._add_aggregation_nodes(data, metric_cols)
            self._add_share_analysis(data, dimension_col, metric_cols)
            self._add_category_analysis(data, dimension_col, metric_cols)

        if len(data) >= 2 and dimension_col and metric_cols:
            self._add_ranking_nodes(data, dimension_col, metric_cols[0])

        self._add_comparison_edges(data, metric_cols)

        return self.graph

    def _parse_sql_result(self, sql_result: str) -> list[dict]:
        try:
            json_match = re.search(r"\[.*\]", sql_result, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
        return []

    def _detect_dimension_column(self, row: dict) -> str | None:
        """ディメンション列を検出"""
        # 1. nameを含むカラムを最優先
        for col in row.keys():
            if "name" in col.lower():
                return col

        # 2. ヒントに一致するカラム（ID以外）
        for col in row.keys():
            col_lower = col.lower()
            if any(hint in col_lower for hint in self.DIMENSION_HINTS):
                if not col_lower.endswith("_id") and col_lower != "id":
                    return col

        # 3. 文字列型のカラム
        for col, val in row.items():
            if isinstance(val, str) and not col.lower().endswith("_id"):
                return col

        return None

    def _detect_metric_columns(self, row: dict) -> list[str]:
        """メトリクス（数値）列を検出"""
        metrics = []
        for col, val in row.items():
            col_lower = col.lower()

            # IDカラムは除外
            if col_lower.endswith("_id") or col_lower == "id":
                continue

            # 数値型かチェック（Decimal含む）
            if val is not None:
                try:
                    float(val)
                    metrics.append(col)
                except (ValueError, TypeError):
                    pass

        # 重要な指標を先にソート
        def metric_priority(col: str) -> int:
            col_lower = col.lower()
            priority_order = [
                "cpa",
                "roas",
                "cpc",
                "cvr",
                "ctr",
                "cost",
                "conversions",
                "clicks",
                "impressions",
            ]
            for i, p in enumerate(priority_order):
                if p in col_lower:
                    return i
            return 100

        return sorted(metrics, key=metric_priority)

    def _get_label(self, col: str) -> str:
        """カラム名からラベルを取得"""
        col_lower = col.lower()

        # 完全一致
        if col_lower in self.METRIC_LABELS:
            return self.METRIC_LABELS[col_lower]

        # 部分一致
        for key, label in self.METRIC_LABELS.items():
            if key in col_lower:
                return label

        # スネークケースを読みやすく変換
        return col.replace("_", " ").title()

    def _format_value(self, value: Any) -> str:
        """値をフォーマット"""
        if value is None:
            return "N/A"
        try:
            num = float(value)
            return EvidenceNode._format_number(num)
        except (ValueError, TypeError):
            return str(value)

    def _add_data_nodes(self, data: list[dict], dimension_col: str | None, metric_cols: list[str]):
        """データ行をノードとして追加"""
        for i, row in enumerate(data):
            parts = []

            # ディメンション値を最初に
            dim_value = None
            if dimension_col and dimension_col in row:
                dim_value = str(row[dimension_col])

            # メトリクス値を追加
            metric_parts = []
            for col in metric_cols:
                if col in row and row[col] is not None:
                    label = self._get_label(col)
                    formatted = self._format_value(row[col])
                    metric_parts.append(f"{label}={formatted}")

            # コンテンツ構築
            if dim_value:
                content = f"{dim_value}: {', '.join(metric_parts)}"
            else:
                # ディメンションがない場合はIDも表示
                id_parts = []
                for col, val in row.items():
                    if col not in metric_cols and val is not None:
                        label = self._get_label(col)
                        id_parts.append(f"{label}={val}")
                content = f"{', '.join(id_parts)}: {', '.join(metric_parts)}"

            node = EvidenceNode.from_sql_row(row, content, i)
            node.metadata["dimension_col"] = dimension_col
            node.metadata["metric_cols"] = metric_cols
            self.graph.add_node(node)

    def _add_aggregation_nodes(self, data: list[dict], metric_cols: list[str]):
        """集計ノードを追加"""
        for col in metric_cols[:5]:
            values = []
            for row in data:
                if col in row and row[col] is not None:
                    try:
                        values.append(float(row[col]))
                    except (ValueError, TypeError):
                        pass

            if not values:
                continue

            label = self._get_label(col)
            total = sum(values)
            avg = total / len(values)
            max_val = max(values)
            min_val = min(values)

            # 基本集計
            self.graph.add_node(EvidenceNode.from_aggregation(label, total, "合計"))
            self.graph.add_node(EvidenceNode.from_aggregation(label, avg, "平均"))
            self.graph.add_node(EvidenceNode.from_aggregation(label, max_val, "最大"))
            self.graph.add_node(EvidenceNode.from_aggregation(label, min_val, "最小"))

            # 追加分析: 最大/最小の比率
            if min_val > 0:
                ratio = max_val / min_val
                self.graph.add_node(
                    EvidenceNode(
                        id=f"analysis_ratio_{col}",
                        type=EvidenceType.COMPARISON,
                        content=f"{label}の最大は最小の{ratio:.1f}倍",
                        value={"ratio": ratio, "metric": col},
                        metadata={"analysis_type": "ratio"},
                    )
                )

            # 追加分析: 標準偏差（ばらつき）
            if len(values) >= 2:
                variance = sum((v - avg) ** 2 for v in values) / len(values)
                std_dev = variance**0.5
                cv = (std_dev / avg * 100) if avg > 0 else 0  # 変動係数

                if cv > 50:
                    dispersion = "非常に大きい"
                elif cv > 30:
                    dispersion = "大きい"
                elif cv > 15:
                    dispersion = "中程度"
                else:
                    dispersion = "小さい"

                self.graph.add_node(
                    EvidenceNode(
                        id=f"analysis_dispersion_{col}",
                        type=EvidenceType.COMPARISON,
                        content=f"{label}のばらつきは{dispersion}（変動係数{cv:.1f}%）",
                        value={"cv": cv, "std_dev": std_dev, "dispersion": dispersion},
                        metadata={"analysis_type": "dispersion"},
                    )
                )

    def _add_share_analysis(
        self, data: list[dict], dimension_col: str | None, metric_cols: list[str]
    ):
        """シェア分析を追加"""
        if not dimension_col or not metric_cols:
            return

        primary_metric = metric_cols[0]
        values_with_names = []

        for row in data:
            if primary_metric in row and row[primary_metric] is not None:
                try:
                    val = float(row[primary_metric])
                    name = row.get(dimension_col, "不明")
                    values_with_names.append((name, val))
                except (ValueError, TypeError):
                    pass

        if not values_with_names:
            return

        total = sum(v for _, v in values_with_names)
        if total <= 0:
            return

        label = self._get_label(primary_metric)

        # 上位の集中度（上位1件のシェア）
        sorted_values = sorted(values_with_names, key=lambda x: x[1], reverse=True)
        top_name, top_value = sorted_values[0]
        top_share = (top_value / total) * 100

        self.graph.add_node(
            EvidenceNode(
                id=f"analysis_top_share_{primary_metric}",
                type=EvidenceType.COMPARISON,
                content=f"トップの「{top_name}」が全体の{top_share:.1f}%を占める",
                value={"top_name": top_name, "share": top_share},
                metadata={"analysis_type": "share"},
            )
        )

        # 上位3件の集中度
        if len(sorted_values) >= 3:
            top3_total = sum(v for _, v in sorted_values[:3])
            top3_share = (top3_total / total) * 100
            self.graph.add_node(
                EvidenceNode(
                    id=f"analysis_top3_share_{primary_metric}",
                    type=EvidenceType.COMPARISON,
                    content=f"上位3件で全体の{top3_share:.1f}%を占める",
                    value={"top3_share": top3_share},
                    metadata={"analysis_type": "concentration"},
                )
            )

    def _add_category_analysis(
        self, data: list[dict], dimension_col: str | None, metric_cols: list[str]
    ):
        """カテゴリ別分析を追加（名前からカテゴリを推測）"""
        if not dimension_col or not metric_cols:
            return

        primary_metric = metric_cols[0]

        # 名前からカテゴリを抽出（_で区切られた最初の部分）
        category_values = {}
        for row in data:
            if primary_metric in row and row[primary_metric] is not None:
                try:
                    val = float(row[primary_metric])
                    name = str(row.get(dimension_col, ""))
                    # カテゴリ抽出（"ECサイトA_ブランド検索" → "ECサイトA"）
                    category = name.split("_")[0] if "_" in name else name
                    if category:
                        if category not in category_values:
                            category_values[category] = []
                        category_values[category].append(val)
                except (ValueError, TypeError):
                    pass

        if len(category_values) < 2:
            return

        label = self._get_label(primary_metric)

        # カテゴリ別の平均を計算
        category_avgs = []
        for cat, vals in category_values.items():
            avg = sum(vals) / len(vals)
            category_avgs.append((cat, avg, len(vals)))

        # 平均でソート
        category_avgs.sort(key=lambda x: x[1])

        # 最も効率の良い/悪いカテゴリ
        best_cat, best_avg, best_count = category_avgs[0]
        worst_cat, worst_avg, worst_count = category_avgs[-1]

        # CPAの場合は低い方が良い
        metric_lower = primary_metric.lower()
        if any(x in metric_lower for x in ["cpa", "cpc", "cost"]):
            self.graph.add_node(
                EvidenceNode(
                    id=f"analysis_best_category_{primary_metric}",
                    type=EvidenceType.COMPARISON,
                    content=f"カテゴリ別では「{best_cat}」系が最も効率的（平均{label}={EvidenceNode._format_number(best_avg)}）",
                    value={"category": best_cat, "avg": best_avg, "count": best_count},
                    metadata={"analysis_type": "category_best"},
                )
            )
            self.graph.add_node(
                EvidenceNode(
                    id=f"analysis_worst_category_{primary_metric}",
                    type=EvidenceType.COMPARISON,
                    content=f"「{worst_cat}」系は改善余地あり（平均{label}={EvidenceNode._format_number(worst_avg)}、{best_cat}系の{worst_avg/best_avg:.1f}倍）",
                    value={"category": worst_cat, "avg": worst_avg, "ratio": worst_avg / best_avg},
                    metadata={"analysis_type": "category_worst"},
                )
            )
        else:
            # CV数などは高い方が良い
            self.graph.add_node(
                EvidenceNode(
                    id=f"analysis_best_category_{primary_metric}",
                    type=EvidenceType.COMPARISON,
                    content=f"カテゴリ別では「{worst_cat}」系が最も高い（平均{label}={EvidenceNode._format_number(worst_avg)}）",
                    value={"category": worst_cat, "avg": worst_avg, "count": worst_count},
                    metadata={"analysis_type": "category_best"},
                )
            )

    def _add_ranking_nodes(self, data: list[dict], dimension_col: str, primary_metric: str):
        """ランキングノードを追加"""
        valid_data = []
        for d in data:
            if primary_metric in d and d[primary_metric] is not None:
                try:
                    val = float(d[primary_metric])
                    valid_data.append((d, val))
                except (ValueError, TypeError):
                    pass

        sorted_data = sorted(valid_data, key=lambda x: x[1], reverse=True)
        metric_label = self._get_label(primary_metric)

        for rank, (row, value) in enumerate(sorted_data[:5], 1):
            item_name = row.get(dimension_col, f"item_{rank}")
            self.graph.add_node(
                EvidenceNode.from_ranking(rank, str(item_name), metric_label, value)
            )

    def _add_comparison_edges(self, data: list[dict], metric_cols: list[str]):
        """比較関係のエッジを追加"""
        if len(data) < 2 or not metric_cols:
            return

        ranking_nodes = sorted(
            self.graph.get_nodes_by_type(EvidenceType.RANKING),
            key=lambda n: n.metadata.get("rank", 999),
        )

        for i in range(len(ranking_nodes) - 1):
            self.graph.add_edge(
                EvidenceEdge(
                    source_id=ranking_nodes[i].id,
                    target_id=ranking_nodes[i + 1].id,
                    relation=RelationType.RANKED_ABOVE,
                    description="順位が上",
                )
            )


def build_evidence_graph(sql_result: str, sql_query: str, question: str) -> EvidenceGraph:
    """ヘルパー関数"""
    return SQLResultGraphBuilder().build(sql_result, sql_query, question)
