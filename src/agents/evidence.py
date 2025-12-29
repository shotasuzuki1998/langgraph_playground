"""
Evidence Graph定義
SQL結果を構造化されたエビデンスとして管理
"""

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EvidenceType(str, Enum):
    """エビデンスの種類"""

    SQL_RESULT = "sql_result"
    AGGREGATION = "aggregation"
    COMPARISON = "comparison"
    RANKING = "ranking"
    CONTEXT = "context"


class RelationType(str, Enum):
    """エビデンス間の関係"""

    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    PART_OF = "part_of"
    RANKED_ABOVE = "ranked_above"


@dataclass
class EvidenceNode:
    """エビデンスノード"""

    id: str
    type: EvidenceType
    content: str
    value: Any
    confidence: float = 1.0
    metadata: dict = field(default_factory=dict)

    @classmethod
    def from_sql_row(cls, row: dict, context: str, row_index: int = 0) -> "EvidenceNode":
        """SQL結果の1行からノードを生成"""
        row_hash = hashlib.md5(json.dumps(row, default=str).encode()).hexdigest()[:8]
        return cls(
            id=f"row_{row_index}_{row_hash}",
            type=EvidenceType.SQL_RESULT,
            content=context,
            value=row,
            metadata={"source": "sql", "row_index": row_index},
        )

    @classmethod
    def from_aggregation(
        cls, name: str, value: float, agg_type: str, unit: str = ""
    ) -> "EvidenceNode":
        """集計値からノードを生成"""
        formatted = cls._format_number(value)
        return cls(
            id=f"agg_{agg_type}_{name}",
            type=EvidenceType.AGGREGATION,
            content=f"{name}の{agg_type}: {formatted}{unit}",
            value=value,
            metadata={"name": name, "agg_type": agg_type, "unit": unit},
        )

    @classmethod
    def from_ranking(cls, rank: int, item_name: str, metric: str, value: float) -> "EvidenceNode":
        """ランキング情報からノードを生成"""
        formatted = cls._format_number(value)
        return cls(
            id=f"rank_{rank}_{item_name[:10]}",
            type=EvidenceType.RANKING,
            content=f"第{rank}位: {item_name}（{metric}={formatted}）",
            value={"rank": rank, "item": item_name, "metric": metric, "value": value},
            metadata={"rank": rank},
        )

    @staticmethod
    def _format_number(value: Any) -> str:
        """数値を読みやすくフォーマット"""
        if isinstance(value, float):
            if abs(value) >= 1_000_000:
                return f"{value/1_000_000:,.2f}M"
            elif abs(value) >= 1_000:
                return f"{value/1_000:,.2f}K"
            elif abs(value) < 1:
                return f"{value:.4f}"
            else:
                return f"{value:,.2f}"
        elif isinstance(value, int):
            if abs(value) >= 1_000_000:
                return f"{value/1_000_000:,.1f}M"
            elif abs(value) >= 1_000:
                return f"{value/1_000:,.1f}K"
            return f"{value:,}"
        return str(value)


@dataclass
class EvidenceEdge:
    """エビデンス間の関係"""

    source_id: str
    target_id: str
    relation: RelationType
    weight: float = 1.0
    description: str = ""


class EvidenceGraph:
    """Evidence Graph本体"""

    def __init__(self):
        self.nodes: dict[str, EvidenceNode] = {}
        self.edges: list[EvidenceEdge] = []
        self.metadata: dict = {}

    def add_node(self, node: EvidenceNode) -> str:
        self.nodes[node.id] = node
        return node.id

    def add_edge(self, edge: EvidenceEdge) -> bool:
        if edge.source_id in self.nodes and edge.target_id in self.nodes:
            self.edges.append(edge)
            return True
        return False

    def get_nodes_by_type(self, etype: EvidenceType) -> list[EvidenceNode]:
        return [n for n in self.nodes.values() if n.type == etype]

    def to_reasoner_prompt(self) -> str:
        """Reasoner（LLM）に渡すプロンプト形式に変換"""
        lines = []

        if "question" in self.metadata:
            lines.append(f"## 質問\n{self.metadata['question']}\n")

        if "sql" in self.metadata:
            lines.append(f"## 実行したSQL\n```sql\n{self.metadata['sql']}\n```\n")

        type_labels = {
            EvidenceType.AGGREGATION: "📊 集計値",
            EvidenceType.RANKING: "🏆 ランキング",
            EvidenceType.SQL_RESULT: "📋 データ",
            EvidenceType.COMPARISON: "📈 分析",
            EvidenceType.CONTEXT: "📌 コンテキスト",
        }

        # 表示順序: 集計値 → 分析 → ランキング → データ
        for etype in [
            EvidenceType.AGGREGATION,
            EvidenceType.COMPARISON,
            EvidenceType.RANKING,
            EvidenceType.SQL_RESULT,
        ]:
            nodes = self.get_nodes_by_type(etype)
            if nodes:
                lines.append(f"### {type_labels.get(etype, etype.value)}")
                for node in nodes:
                    conf = f" (確信度:{node.confidence:.0%})" if node.confidence < 1.0 else ""
                    lines.append(f"- [{node.id}] {node.content}{conf}")
                lines.append("")

        if self.edges:
            lines.append("### 🔗 エビデンス間の関係")
            relation_symbols = {
                RelationType.GREATER_THAN: ">",
                RelationType.LESS_THAN: "<",
                RelationType.RANKED_ABOVE: "↑",
                RelationType.PART_OF: "⊂",
            }
            for edge in self.edges:
                symbol = relation_symbols.get(edge.relation, "→")
                src_content = self.nodes[edge.source_id].content[:30]
                tgt_content = self.nodes[edge.target_id].content[:30]
                lines.append(f"- {src_content}... {symbol} {tgt_content}...")
            lines.append("")

        stats = self.get_stats()
        lines.append(f"### 📈 グラフ統計")
        lines.append(f"- エビデンス数: {stats['total_nodes']}")
        lines.append(f"- 関係数: {stats['total_edges']}")

        return "\n".join(lines)

    def get_stats(self) -> dict:
        type_counts = {}
        for node in self.nodes.values():
            type_counts[node.type.value] = type_counts.get(node.type.value, 0) + 1

        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "node_types": type_counts,
            "avg_confidence": (
                sum(n.confidence for n in self.nodes.values()) / len(self.nodes)
                if self.nodes
                else 0
            ),
        }

    def to_dict(self) -> dict:
        return {
            "nodes": {
                nid: {
                    "id": n.id,
                    "type": n.type.value,
                    "content": n.content,
                    "value": n.value,
                    "confidence": n.confidence,
                    "metadata": n.metadata,
                }
                for nid, n in self.nodes.items()
            },
            "edges": [
                {
                    "source_id": e.source_id,
                    "target_id": e.target_id,
                    "relation": e.relation.value,
                    "weight": e.weight,
                    "description": e.description,
                }
                for e in self.edges
            ],
            "metadata": self.metadata,
            "stats": self.get_stats(),
        }
