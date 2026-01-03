"""
Evidence Builder（エビデンス構築モジュール）

【目的】
SQL実行結果を構造化・分析し、LLMが正確な回答を生成できるようにする。
LLMに計算させず、Pythonで事前計算することでハルシネーションを防止する。

【処理フロー】
1. SQL結果（JSON文字列）をパース
2. 派生指標（CTR/CPC/CVR/CPA/ROAS）を自動計算
3. カラムの役割（dimension/metrics）を判定
4. 各種分析（集計/統計/ランキング/シェア/カテゴリ/期間比較）を実行
5. Evidenceオブジェクトに格納
6. to_prompt()でLLM向けのMarkdown文字列に変換

【主な機能】
- 派生指標追加（CTR/CPC/CVR/CPA/ROAS）
- dimension検出改善（DDL前提の安定ルール）
- 期間比較（dateが"日別集計っぽい"ときだけ）
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

# ============================================================================
# データモデル（Evidenceクラス）
# ============================================================================


@dataclass
class Evidence:
    """
    構造化されたエビデンス（分析結果を格納する箱）

    【役割】
    各ヘルパー関数が計算した結果を1つにまとめて保持する。
    to_prompt()でLLM向けに変換、to_dict()でデバッグ用に変換できる。

    【フィールド説明】
    - question: ユーザーの元の質問
    - sql: 実行したSQLクエリ
    - row_count: 結果の行数
    - raw_data: SQL結果の生データ（辞書のリスト）
    - aggregations: 集計値（合計/平均/最大/最小）
    - analysis: 統計分析結果（比率/変動係数）
    - rankings: ランキング（上位5件）
    - category_analysis: カテゴリ別比較（最良/最悪）
    - share_analysis: シェア分析（トップシェア/上位3件集中度）
    - period_comparison: 期間比較（前半/後半の変化率）
    """

    # === 基本情報（必須） ===
    question: str  # ユーザーの質問文
    sql: str  # 実行したSQLクエリ
    row_count: int  # 結果の行数

    # === データ ===
    raw_data: list[dict] = field(default_factory=list)
    # SQL結果の生データ
    # 例: [{"service_name": "ECサイトA", "total_cost": "21325991.55"}, ...]

    # === 集計値 ===
    aggregations: dict[str, dict[str, float]] = field(default_factory=dict)
    # 各メトリクスの合計/平均/最大/最小
    # 例: {"費用": {"合計": 200000000, "平均": 66000000, "最大": 92000000, "最小": 21000000}}
    #
    # 【注意】派生指標（CTR/CPC/CVR/CPA/ROAS）は「合計」を出さない
    #        合計すると意味のない値になるため

    # === 統計分析結果 ===
    analysis: list[str] = field(default_factory=list)
    # 比率やばらつきの分析コメント
    # 例: ["最大は最小の4.3倍", "ばらつきは大きい（変動係数53%）"]
    #
    # 【必要条件】2件以上のデータが必要（比較するため）

    # === ランキング ===
    rankings: list[dict] = field(default_factory=list)
    # 値の大きい順に並べた上位5件
    # 例: [{"rank": 1, "name": "転職C", "metric": "費用", "value": 92000000}, ...]
    #
    # 【必要条件】dimension（名前列）が必要（「誰が」1位かを表示するため）

    # === カテゴリ分析 ===
    category_analysis: dict = field(default_factory=dict)
    # 名前から抽出したカテゴリ別の比較
    # 例: {"best": {"name": "ECサイト", "avg": 1000}, "worst": {"name": "転職", "avg": 3000, "ratio": 3.0}}
    #
    # 【処理】名前を"_"で分割して先頭をカテゴリとする
    # 【注意】CPAなどは低い方が良いので、best/worstの判定を指標に応じて切り替える

    # === シェア分析 ===
    share_analysis: dict = field(default_factory=dict)
    # トップの占有率と上位3件の集中度
    # 例: {"top_name": "転職C", "top_share": 46.1, "top3_share": 100.0}
    #
    # 【ビジネス上の意味】
    # - トップシェアが高い → 特定項目に依存、リスクあり
    # - 上位3件で90%以上 → 寡占状態

    # === 期間比較（新機能） ===
    period_comparison: dict = field(default_factory=dict)
    # 日別データの前半/後半の比較
    # 例: {
    #   "prev_range": {"start": "2024-01-01", "end": "2024-01-15"},
    #   "curr_range": {"start": "2024-01-16", "end": "2024-01-31"},
    #   "metrics": {"費用": {"prev": 100000, "curr": 120000, "change_pct": 20.0}},
    #   "summary": "費用は+20.0%, CV数は+15.0%"
    # }
    #
    # 【発動条件】日別集計っぽいデータ（date列があり、ユニーク日付が多い）

    def to_dict(self) -> dict:
        """
        辞書形式に変換（デバッグ用）

        【用途】
        - JSONとして保存・確認できる形式
        - 開発者がデータを確認するため
        - 数値は生の値のまま（フォーマットなし）
        """
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
        """
        LLMに渡すプロンプト形式に変換

        【用途】
        - LLMが読みやすいMarkdown形式に変換
        - 数値は読みやすい形式（200.71Mなど）
        - データがないセクションは出力しない

        【処理】
        1. 各セクションをpartsリストに追加
        2. 最後に改行で結合して1つの文字列に
        """
        # === 基本情報（常に出力） ===
        parts = [
            f"## 質問\n{self.question}",
            f"\n## 実行SQL\n\n{self.sql}\n```",
            f"\n## 結果行数: {self.row_count}件",
        ]

        # === 集計結果（データがある場合のみ） ===
        if self.aggregations:
            parts.append("\n## 集計結果")
            for metric, values in self.aggregations.items():
                # {"合計": 200000000, "平均": 66000000} → "合計: 200000000.00, 平均: 66000000.00"
                vals_str = ", ".join(f"{k}: {v:.2f}" for k, v in values.items())
                parts.append(f"- {metric}: {vals_str}")

        # === 統計分析（データがある場合のみ） ===
        if self.analysis:
            parts.append("\n## 分析")
            for a in self.analysis:
                parts.append(f"- {a}")

        # === ランキング（データがある場合のみ） ===
        if self.rankings:
            parts.append("\n## ランキング")
            for r in self.rankings:
                parts.append(f"- {r['rank']}位: {r['name']} ({r['metric']}: {r['value']:.2f})")

        # === シェア分析（データがある場合のみ） ===
        if self.share_analysis:
            parts.append("\n## シェア分析")
            parts.append(
                f"- トップ: {self.share_analysis.get('top_name')} ({self.share_analysis.get('top_share', 0):.1f}%)"
            )
            if "top3_share" in self.share_analysis:
                parts.append(f"- 上位3件合計: {self.share_analysis['top3_share']:.1f}%")

        # === カテゴリ分析（データがある場合のみ） ===
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

        # === 期間比較（データがある場合のみ） ===
        if self.period_comparison:
            parts.append("\n## 期間比較")
            if "summary" in self.period_comparison:
                parts.append(f"- {self.period_comparison['summary']}")

        # === 結合して返す ===
        # 各要素を改行で連結して1つの文字列に
        # 【重要】この return 文がないとLLMに何も渡されず、ハルシネーションが発生する
        return "\n".join(parts)


# ============================================================================
# ヘルパー関数（ユーティリティ）
# ============================================================================


def _parse_sql_result(sql_result: str) -> list[dict]:
    """
    SQL結果（JSON文字列）をパースして辞書のリストに変換

    【入力例】
    "結果: 3件\n[{\"service_name\": \"ECサイトA\", \"total_cost\": \"21325991.55\"}, ...]"

    【出力例】
    [{"service_name": "ECサイトA", "total_cost": "21325991.55"}, ...]

    【処理】
    1. 正規表現で [...] 部分を抽出（ログ混在にも耐える）
    2. json.loads()でPythonリストに変換
    """
    try:
        # 正規表現で [...] 部分を抽出
        # re.DOTALL: .が改行にもマッチする
        match = re.search(r"\[.*\]", sql_result, re.DOTALL)
        if match:
            loaded = json.loads(match.group())
            return loaded if isinstance(loaded, list) else []
    except json.JSONDecodeError:
        pass
    return []


def _safe_float(x: Any) -> Optional[float]:
    """
    値を安全にfloatに変換

    【用途】
    SQL結果の値は文字列の場合があるため、安全に変換する

    【入力例】
    "21325991.55" → 21325991.55
    "ECサイトA" → None
    None → None
    """
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def _is_date_like(x: Any) -> bool:
    """
    値が日付っぽいかどうかを判定

    【判定基準】
    YYYY-MM-DD 形式の文字列かどうか

    【入力例】
    "2024-01-15" → True
    "ECサイトA" → False
    None → False
    """
    if x is None or not isinstance(x, str):
        return False
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", x.strip()))


def _get_label(col: str) -> str:
    """
    カラム名から日本語ラベルを取得

    【用途】
    "total_cost" → "費用" のように、ユーザーに分かりやすいラベルに変換

    【処理】
    1. 定義済みラベルがあればそれを返す
    2. なければカラム名をタイトルケースに変換

    【注意】
    旧バージョンより簡略化されている。必要に応じてパターンマッチング版を使用。
    """
    col_lower = col.lower()

    # 定義済みラベル
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

    # 定義済みならそれを返す、なければタイトルケースに変換
    return labels.get(col_lower, col.replace("_", " ").title())


# ============================================================================
# 1) 派生指標の自動計算
# ============================================================================


def _add_derived_metrics(data: list[dict]) -> list[dict]:
    """
    基本指標から派生指標（CTR/CPC/CVR/CPA/ROAS）を自動計算して追加

    【用途】
    SQLで派生指標を計算していなくても、基本指標があれば自動で計算する

    【計算式】
    - CTR = clicks / impressions（クリック率）
    - CPC = cost / clicks（クリック単価）
    - CVR = conversions / clicks（コンバージョン率）
    - CPA = cost / conversions（顧客獲得単価）
    - ROAS = conversion_value / cost（広告費用対効果）

    【入力】
    [{"impressions": 1000, "clicks": 50, "cost": 10000, "conversions": 5}, ...]

    【出力】
    [{"impressions": 1000, "clicks": 50, "cost": 10000, "conversions": 5,
      "ctr": 0.05, "cpc": 200, "cvr": 0.1, "cpa": 2000}, ...]
    """
    out: list[dict] = []

    for row in data:
        # 元の行をコピー（元データを変更しない）
        r = dict(row)

        # 基本指標を取得
        imp = _safe_float(r.get("impressions"))  # 表示回数
        clk = _safe_float(r.get("clicks"))  # クリック数
        cost = _safe_float(r.get("cost"))  # 費用
        conv = _safe_float(r.get("conversions"))  # コンバージョン数
        convv = _safe_float(r.get("conversion_value"))  # コンバージョン価値

        # CTR = clicks / impressions（クリック率）
        if imp and imp > 0 and clk is not None:
            r["ctr"] = clk / imp

        # CPC = cost / clicks（クリック単価）
        if clk and clk > 0 and cost is not None:
            r["cpc"] = cost / clk

        # CVR = conversions / clicks（コンバージョン率）
        if clk and clk > 0 and conv is not None:
            r["cvr"] = conv / clk

        # CPA = cost / conversions（顧客獲得単価）
        if conv and conv > 0 and cost is not None:
            r["cpa"] = cost / conv

        # ROAS = conversion_value / cost（広告費用対効果）
        if cost and cost > 0 and convv is not None:
            r["roas"] = convv / cost

        out.append(r)

    return out


# ============================================================================
# 2) カラム種別判定（Dimension/Metrics検出）
# ============================================================================


# Dimension（名前列）の優先順位
# 上にあるほど優先的にdimensionとして選択される
DIMENSION_PRIORITY = [
    "campaign_name",  # キャンペーン名
    "ad_group_name",  # 広告グループ名
    "keyword_text",  # キーワード
    "query_text",  # 検索クエリ
    "service_name",  # サービス名
    "campaign_type",  # キャンペーンタイプ
    "match_type",  # マッチタイプ
    "status",  # ステータス
    "targeting_type",  # ターゲティングタイプ
    "targeting_value",  # ターゲティング値
    "date",  # 日付（最後寄り：ランキングが日付になるのを避けたい）
    "name",  # 汎用的なname
]


def _detect_dimension_and_metrics(row: dict) -> tuple[Optional[str], list[str]]:
    """
    1行目のデータからdimension（名前列）とmetrics（数値列）を検出

    【用途】
    SQLの結果から「名前の列」と「数値の列」を自動判別する

    【判定ルール】
    - ID/GoogleIDで始まる列 → 除外（表示軸になりにくい）
    - 日付列 → dimension候補
    - 数値に変換可能 → metrics
    - それ以外 → dimension候補
    - dimension候補が複数ある場合 → DIMENSION_PRIORITYで優先順位を決定

    【入力例】
    {"campaign_name": "ブランド検索", "total_cost": "1000000", "clicks": "5000"}

    【出力例】
    ("campaign_name", ["total_cost", "clicks"])
    """
    metrics: list[str] = []
    non_numeric_cols: list[str] = []

    for col, val in row.items():
        cl = col.lower()

        # ID/GoogleIDは除外（表示軸になりにくい）
        # 例: campaign_id, google_campaign_id
        if cl == "id" or cl.endswith("_id") or cl.startswith("google_"):
            continue

        # dateは次元候補
        if cl == "date" or _is_date_like(val):
            non_numeric_cols.append(col)
            continue

        # 数値に変換できるか試す
        f = _safe_float(val)
        if f is not None:
            # 数値 → metrics
            metrics.append(col)
        else:
            # 文字列 → dimension候補
            non_numeric_cols.append(col)

    # dimensionを優先順位で決定
    # 実際のカラム名（大文字小文字を保持）を取得するための辞書
    lowers = {c.lower(): c for c in row.keys()}

    dimension: Optional[str] = None
    for key in DIMENSION_PRIORITY:
        if key in lowers:
            dimension = lowers[key]
            break

    # 優先リストにない場合は最初の非数値列を使用
    if dimension is None:
        dimension = non_numeric_cols[0] if non_numeric_cols else None

    # metricsの並び順を安定化（重要指標優先）
    # CPA/ROASなど重要な指標を先に持ってくる
    priority = [
        "cpa",  # 顧客獲得単価（最重要）
        "roas",  # 広告費用対効果
        "cpc",  # クリック単価
        "cvr",  # コンバージョン率
        "ctr",  # クリック率
        "cost",  # 費用
        "conversions",  # コンバージョン数
        "clicks",  # クリック数
        "impressions",  # 表示回数
        "conversion_value",  # コンバージョン価値
    ]

    def metric_priority(col: str) -> int:
        """指標の優先度を返す（小さいほど優先）"""
        c = col.lower()
        for i, p in enumerate(priority):
            if c == p or p in c:
                return i
        return 100  # リストにない場合は最後

    metrics = sorted(metrics, key=metric_priority)

    return dimension, metrics


# ============================================================================
# 3) 分析関数群（集計/統計/ランキング/シェア/カテゴリ）
# ============================================================================


def _calculate_aggregations(
    data: list[dict], metric_cols: list[str]
) -> dict[str, dict[str, float]]:
    """
    各メトリクスの集計値（合計/平均/最大/最小）を計算

    【必要条件】
    - データが1件以上
    - metricsが1つ以上

    【注意】
    派生指標（CTR/CPC/CVR/CPA/ROAS）は「合計」を出さない
    → 合計すると意味のない値になるため
    → 例: CPAの合計 = 意味がない

    【入力例】
    data = [{"total_cost": 21M}, {"total_cost": 87M}, {"total_cost": 92M}]
    metric_cols = ["total_cost"]

    【出力例】
    {"費用": {"合計": 200M, "平均": 66.7M, "最大": 92M, "最小": 21M}}
    """
    result: dict[str, dict[str, float]] = {}

    # 派生指標のリスト（これらは「合計」を出さない）
    derived = {"ctr", "cpc", "cvr", "cpa", "roas"}

    # 上位7指標まで処理（多すぎると見づらい）
    for col in metric_cols[:7]:
        values: list[float] = []

        # 各行から値を抽出
        for row in data:
            v = _safe_float(row.get(col))
            if v is not None:
                values.append(v)

        if not values:
            continue

        label = _get_label(col)

        # 派生指標は平均/最大/最小のみ（合計は意味がない）
        if col.lower() in derived:
            result[label] = {
                "平均": sum(values) / len(values),
                "最大": max(values),
                "最小": min(values),
            }
        else:
            # 通常の指標は合計も含める
            result[label] = {
                "合計": sum(values),
                "平均": sum(values) / len(values),
                "最大": max(values),
                "最小": min(values),
            }

    return result


def _generate_analysis(data: list[dict], metric_cols: list[str]) -> list[str]:
    """
    統計分析（比率・変動係数）を実行してコメントを生成

    【必要条件】
    - データが2件以上（比較するため）
    - metricsが1つ以上

    【計算内容】
    1. 比率: 最大 / 最小 = 何倍か
    2. 変動係数: (標準偏差 / 平均) × 100 = ばらつきの大きさ

    【変動係数の判定基準】
    - > 50%: 非常に大きい
    - > 30%: 大きい
    - > 15%: 中程度
    - ≤ 15%: 小さい

    【出力例】
    ["費用の最大は最小の4.3倍", "費用のばらつきは大きい（変動係数48.3%）"]
    """
    analysis: list[str] = []

    # 上位4指標まで処理
    for col in metric_cols[:4]:
        values = []

        # 各行から値を抽出
        for row in data:
            v = _safe_float(row.get(col))
            if v is not None:
                values.append(v)

        # 2件以上ないと比較できない
        if len(values) < 2:
            continue

        label = _get_label(col)
        mx, mn = max(values), min(values)
        avg = sum(values) / len(values)

        # === 比率分析 ===
        # 最大は最小の何倍か
        if mn > 0:
            analysis.append(f"{label}の最大は最小の{(mx / mn):.1f}倍")

        # === ばらつき分析（変動係数） ===
        if avg > 0:
            # 分散 = Σ(値 - 平均)² / n
            var = sum((v - avg) ** 2 for v in values) / len(values)
            # 標準偏差 = √分散
            std = var**0.5
            # 変動係数 = (標準偏差 / 平均) × 100
            cv = (std / avg) * 100

            # 変動係数の判定
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
    """
    値で並べ替えてランキング（上位5件）を生成

    【必要条件】
    - dimensionが存在する（「誰が」1位かを表示するため）
    - metricsが1つ以上

    【処理】
    1. 名前と値のペアを抽出
    2. 値で降順ソート
    3. 上位5件に順位を付与

    【出力例】
    [
        {"rank": 1, "name": "転職サービスC", "metric": "費用", "value": 92429185.32},
        {"rank": 2, "name": "SaaSプロダクトB", "metric": "費用", "value": 86957282.50},
        ...
    ]
    """
    if not dimension:
        return []

    valid = []

    # 名前と値のペアを抽出
    for row in data:
        v = _safe_float(row.get(metric))
        if v is None:
            continue
        valid.append({"name": str(row.get(dimension, "不明")), "value": v})

    # 値で降順ソート
    valid.sort(key=lambda x: x["value"], reverse=True)

    # ラベル変換
    metric_label = _get_label(metric)

    # 上位5件に順位を付与して返す
    return [
        {"rank": i + 1, "name": r["name"], "metric": metric_label, "value": r["value"]}
        for i, r in enumerate(valid[:5])
    ]


def _calculate_share_analysis(data: list[dict], dimension: Optional[str], metric: str) -> dict:
    """
    シェア分析（トップの占有率と上位3件の集中度）を計算

    【必要条件】
    - dimensionが存在する
    - データが2件以上（1件だと100%になり意味がない）

    【計算内容】
    - トップシェア = (1位の値 / 合計) × 100
    - 上位3件シェア = (上位3件の合計 / 全体合計) × 100

    【ビジネス上の意味】
    - トップシェアが高い（例: 80%）→ 特定項目に依存、リスクあり
    - 上位3件で90%以上 → 寡占状態

    【出力例】
    {"top_name": "転職サービスC", "top_share": 46.1, "top3_share": 100.0}
    """
    if not dimension:
        return {}

    pairs = []

    # 名前と値のペアを抽出
    for row in data:
        v = _safe_float(row.get(metric))
        if v is None:
            continue
        pairs.append((str(row.get(dimension, "不明")), v))

    if not pairs:
        return {}

    # 合計を計算
    total = sum(v for _, v in pairs)
    if total <= 0:
        return {}

    # 値で降順ソート
    pairs.sort(key=lambda x: x[1], reverse=True)

    # トップのシェアを計算
    top_name, top_val = pairs[0]
    out = {"top_name": top_name, "top_share": (top_val / total) * 100}

    # 上位3件のシェアを計算（3件以上ある場合）
    if len(pairs) >= 3:
        top3 = sum(v for _, v in pairs[:3])
        out["top3_share"] = (top3 / total) * 100

    return out


def _calculate_category_analysis(data: list[dict], dimension: Optional[str], metric: str) -> dict:
    """
    カテゴリ別分析（名前からカテゴリを抽出して比較）

    【必要条件】
    - dimensionが存在する
    - 2カテゴリ以上（比較するため）

    【処理】
    1. 名前を"_"で分割して先頭をカテゴリとする
       例: "ECサイトA_ブランド検索" → "ECサイトA"
    2. カテゴリごとに平均を計算
    3. 最良/最悪カテゴリを判定

    【注意】
    CPAなどは低い方が良いので、best/worstの判定を指標に応じて切り替える
    - CPA/CPC/cost → 低い方がbest
    - CV数/ROAS → 高い方がbest

    【出力例】
    {
        "best": {"name": "ECサイトA", "avg": 1044.45},
        "worst": {"name": "転職", "avg": 3333.34, "ratio": 3.2}
    }
    """
    if not dimension:
        return {}

    cat_vals: dict[str, list[float]] = {}

    # カテゴリごとに値をグループ化
    for row in data:
        v = _safe_float(row.get(metric))
        if v is None:
            continue

        name = str(row.get(dimension, ""))

        # 名前を"_"で分割して先頭をカテゴリとする
        cat = name.split("_")[0] if "_" in name else name

        if not cat:
            continue

        # カテゴリに値を追加
        cat_vals.setdefault(cat, []).append(v)

    # 2カテゴリ以上ないと比較できない
    if len(cat_vals) < 2:
        return {}

    # カテゴリ別の平均を計算
    avgs = [(cat, sum(vals) / len(vals)) for cat, vals in cat_vals.items()]

    # 平均でソート（昇順）
    avgs.sort(key=lambda x: x[1])

    best_cat, best_avg = avgs[0]  # 最小
    worst_cat, worst_avg = avgs[-1]  # 最大

    # CPAなどは低い方が良い
    ml = metric.lower()
    lower_is_better = any(x in ml for x in ["cpa", "cpc", "cost"])

    if lower_is_better:
        # 低い方が良い → 最小がbest、最大がworst
        return {
            "best": {"name": best_cat, "avg": best_avg},
            "worst": {
                "name": worst_cat,
                "avg": worst_avg,
                "ratio": (worst_avg / best_avg) if best_avg > 0 else 0,
            },
        }
    else:
        # 高い方が良い → 最大がbest、最小がworst
        return {
            "best": {"name": worst_cat, "avg": worst_avg},
            "worst": {
                "name": best_cat,
                "avg": best_avg,
                "ratio": (worst_avg / best_avg) if best_avg > 0 else 0,
            },
        }


# ============================================================================
# 4) 期間比較（日別データの前半/後半比較）
# ============================================================================


def _calculate_period_comparison(data: list[dict], metric_cols: list[str]) -> dict:
    """
    期間比較（日別データの前半/後半を比較して変化率を計算）

    【発動条件】
    - date列が存在する
    - 日別集計っぽいデータ（ユニーク日付が多い）
    - 4行以上（前半/後半に分けるため）

    【処理】
    1. date列を探す
    2. 日付でソート
    3. 前半/後半に分割
    4. 各指標の変化率を計算

    【出力例】
    {
        "date_col": "date",
        "prev_range": {"start": "2024-01-01", "end": "2024-01-15"},
        "curr_range": {"start": "2024-01-16", "end": "2024-01-31"},
        "metrics": {
            "費用": {"prev": 100000, "curr": 120000, "change": 20000, "change_pct": 20.0}
        },
        "summary": "費用は+20.0%, CV数は+15.0%"
    }
    """
    # === date列を探す ===
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

    # === 日付っぽい行だけ抽出してソート ===
    rows = [r for r in data if _is_date_like(r.get(date_col))]
    if len(rows) < 4:
        return {}

    rows.sort(key=lambda r: str(r.get(date_col)))
    unique_dates = len({r.get(date_col) for r in rows})

    # === 日別集計っぽいか簡易判定 ===
    # 行数に対してユニーク日付が十分多い（= dateが主要な軸）
    # 例: 30行で10日 → OK、30行で3日 → NG（日付以外の軸がある）
    if unique_dates < max(3, len(rows) // 3):
        return {}

    # === 前半/後半に分割 ===
    mid = len(rows) // 2
    prev, curr = rows[:mid], rows[mid:]
    if not prev or not curr:
        return {}

    def sum_metric(rs: list[dict], col: str) -> float:
        """指定された行リストから指標の合計を計算"""
        s = 0.0
        for r in rs:
            v = _safe_float(r.get(col))
            if v is not None:
                s += v
        return s

    # === 各指標の変化率を計算 ===
    target_cols = metric_cols[:5]  # 上位5指標
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
        # 変化率 = (変化量 / 前期) × 100
        change_pct = ((change) / abs(prev_sum) * 100) if prev_sum != 0 else None

        out["metrics"][_get_label(col)] = {
            "prev": prev_sum,
            "curr": curr_sum,
            "change": change,
            "change_pct": change_pct,
        }

    # === ざっくり要約を生成 ===
    summary = []
    for k in ["費用", "CV数", "ROAS", "CPA", "クリック数", "表示回数"]:
        m = out["metrics"].get(k)
        if m and m["change_pct"] is not None:
            # +20.0% または -15.0% の形式
            summary.append(f"{k}は{m['change_pct']:+.1f}%")

    if summary:
        out["summary"] = "、".join(summary)

    return out


# ============================================================================
# メイン関数（エントリーポイント）
# ============================================================================


def build_evidence(sql_result: str, sql_query: str, question: str) -> Evidence:
    """
    SQL結果からEvidenceを構築するメイン関数

    【処理フロー】
    1. SQL結果をパース
    2. 派生指標を追加
    3. カラム種別を判定
    4. 各種分析を実行
    5. Evidenceオブジェクトを構築して返す

    【入力】
    - sql_result: SQL実行結果（JSON文字列）
    - sql_query: 実行したSQLクエリ
    - question: ユーザーの質問

    【出力】
    - Evidence: 構造化されたエビデンス

    【使用例】
    evidence = build_evidence(sql_result, sql_query, question)
    prompt = evidence.to_prompt()  # LLM向けに変換
    """
    # === 1. SQL結果をパース ===
    data = _parse_sql_result(sql_result)

    # データがない場合は空のEvidenceを返す
    if not data:
        return Evidence(question=question, sql=sql_query, row_count=0)

    # === 2. 派生指標を追加 ===
    # CTR/CPC/CVR/CPA/ROASを自動計算
    data = _add_derived_metrics(data)

    # === 3. カラム種別を判定 ===
    # dimension（名前列）とmetrics（数値列）を検出
    dimension, metrics = _detect_dimension_and_metrics(data[0])

    # === 4. 各種分析を実行 ===
    aggregations = {}
    analysis = []
    rankings = []
    share_analysis = {}
    category_analysis = {}
    period_comparison = {}

    # 4-1. 集計（metricsがあれば）
    # 条件: データ1件以上 + metrics1つ以上
    if metrics:
        aggregations = _calculate_aggregations(data, metrics)

    # 4-2. ばらつき分析（2行以上推奨）
    # 条件: データ2件以上 + metrics1つ以上
    if len(data) >= 2 and metrics:
        analysis = _generate_analysis(data, metrics)

    # 4-3. dimension必須の分析
    # 条件: dimension存在 + metrics1つ以上
    if dimension and metrics:
        # ランキング
        rankings = _generate_rankings(data, dimension, metrics[0])
        # シェア分析
        share_analysis = _calculate_share_analysis(data, dimension, metrics[0])
        # カテゴリ分析
        category_analysis = _calculate_category_analysis(data, dimension, metrics[0])

    # 4-4. 期間比較（日別っぽいときだけ）
    if metrics:
        period_comparison = _calculate_period_comparison(data, metrics)

    # === 5. Evidenceオブジェクトを構築 ===
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
