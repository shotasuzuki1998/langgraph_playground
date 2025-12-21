"""
Google広告データベース用 SQLクエリチェッカー
生成されたSQLの安全性とポリシー準拠をチェックします
"""

import re

from src.settings import settings

# アクセス許可テーブル
ALLOWED_TABLES = {
    # マスターデータ
    "services",
    "ad_accounts",
    "campaigns",
    "ad_groups",
    "keywords",
    "ads",
    "targeting_settings",
    "search_queries",
    # 実績データ
    "search_query_keyword_ad_daily_stats",
    "display_ad_daily_stats",
    "campaign_daily_stats",
}

# 禁止キーワード（DML/DDL）
DENY_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|REPLACE|TRUNCATE|GRANT|REVOKE)\b",
    re.I,
)

# 危険な構文（コメント、セミコロン複数）
DANGEROUS_RE = re.compile(
    r"(--|#|/\*|\*/|;\s*\w)",  # コメントや複数文
    re.I,
)

# LIMIT句の検出（末尾）
HAS_LIMIT_RE = re.compile(
    r"\bLIMIT\s+(\d+)(?:\s*,\s*\d+)?\s*;?\s*$",
    re.I,
)

# 非プレーンSQLの検出（CTE、サブクエリ、UNION等）
NON_PLAIN_SQL_RE = re.compile(
    r"\b(WITH\s+\w+\s+AS|UNION|INTERSECT|EXCEPT)\b",
    re.I,
)

# サブクエリの検出（SELECT内SELECT）- JOINのサブクエリは許可しない
SUBQUERY_RE = re.compile(
    r"\(\s*SELECT\b",
    re.I,
)

# FROM/JOIN句からテーブル名を抽出
FROM_RE = re.compile(
    r"\bFROM\s+([`\"\[]?\w+[`\"\]]?)(?:\s+(?:AS\s+)?(\w+))?",
    re.I,
)
JOIN_RE = re.compile(
    r"\bJOIN\s+([`\"\[]?\w+[`\"\]]?)(?:\s+(?:AS\s+)?(\w+))?",
    re.I,
)


# 以下ヘルパー関数たち。
def _normalize_identifier(name: str) -> str:
    """テーブル名を正規化（引用符除去、小文字化）"""
    return re.sub(r'^[`"\[]|[`"\]]$', "", name).lower()


def _extract_tables(query: str) -> tuple[set[str], dict[str, str]]:
    tables = set()

    for pattern in [FROM_RE, JOIN_RE]:
        for match in pattern.finditer(query):
            table = _normalize_identifier(match.group(1))
            tables.add(table)

    return tables


def _extract_limit(query: str) -> int | None:
    """LIMIT値を抽出"""
    match = HAS_LIMIT_RE.search(query)
    if match:
        return int(match.group(1))
    return None


# 実際にクエリチェックの判定をオブジェクトとして持つクラス。
class QueryCheckResult:
    """クエリチェックの結果"""

    def __init__(self, is_valid: bool, query: str = "", error: str = ""):
        self.is_valid = is_valid
        self.query = query  # 修正後のクエリ（LIMITの追加など）
        self.error = error  # エラーメッセージ

    def __repr__(self):
        if self.is_valid:
            return f"QueryCheckResult(valid=True, query='{self.query[:50]}...')"
        return f"QueryCheckResult(valid=False, error='{self.error}')"


# 実際にクエリチェックを処理する関数
def check_query(query: str, allow_subqueries: bool = False) -> QueryCheckResult:
    """
    SQLクエリの安全性とポリシー準拠をチェック

    Args:
        query: チェックするSQLクエリ
        service_id: サービスIDでフィルタを強制する場合に指定
        allow_subqueries: サブクエリを許可するか
        max_limit: 許可する最大LIMIT値（Noneの場合はsettingsから取得）

    Returns:
        QueryCheckResult: チェック結果
    """
    if max_limit is None:
        max_limit = settings.max_limit

    # 1. 基本的な正規化
    query = query.strip()

    # 2. 空クエリチェック
    if not query:
        return QueryCheckResult(False, error="クエリが空です")

    # 3. 複数文チェック（セミコロンが途中にある）
    semicolon_count = query.count(";")
    if semicolon_count > 1:
        return QueryCheckResult(False, error="複数のSQL文は許可されていません")
    if semicolon_count == 1 and not query.rstrip().endswith(";"):
        return QueryCheckResult(False, error="複数のSQL文は許可されていません")

    # 末尾のセミコロンを除去
    query = query.rstrip(";").strip()

    # 4. SELECTのみ許可
    if not query.upper().startswith("SELECT"):
        return QueryCheckResult(False, error="SELECT文のみ実行可能です")

    # 5. DML/DDLの検出
    if DENY_RE.search(query):
        return QueryCheckResult(
            False,
            error="INSERT/UPDATE/DELETE/ALTER/DROP等のDML/DDL文は許可されていません",
        )

    # 6. 危険な構文の検出
    if DANGEROUS_RE.search(query):
        return QueryCheckResult(
            False,
            error="SQLコメントや複数文の実行は許可されていません",
        )

    # 7. サブクエリ/CTE/UNION等の検出
    if NON_PLAIN_SQL_RE.search(query):
        return QueryCheckResult(
            False,
            error="WITH句(CTE)、UNION、INTERSECT、EXCEPTは許可されていません",
        )

    if not allow_subqueries and SUBQUERY_RE.search(query):
        return QueryCheckResult(
            False,
            error="サブクエリは許可されていません",
        )

    # 8. テーブル名の抽出と検証
    tables = _extract_tables(query)

    if not tables:
        return QueryCheckResult(False, error="テーブル名を特定できませんでした")

    # 許可されていないテーブルへのアクセスチェック
    disallowed = tables - ALLOWED_TABLES
    if disallowed:
        return QueryCheckResult(
            False,
            error=f"アクセスが許可されていないテーブル: {', '.join(sorted(disallowed))}",
        )

    # 9. LIMIT句の処理
    current_limit = _extract_limit(query)

    if current_limit is None:
        # LIMITがない場合はデフォルトを追加
        query = f"{query} LIMIT {settings.default_limit}"
    elif current_limit > settings.max_limit:
        # 最大値を超えている場合は制限
        query = HAS_LIMIT_RE.sub(f"LIMIT {settings.max_limit}", query)

    return QueryCheckResult(True, query=query)
