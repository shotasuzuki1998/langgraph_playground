"""
Google広告データベース用 SQLクエリチェッカー
生成されたSQLの安全性とポリシー準拠をチェックします
"""

import re

from src.settings import settings

# =============================================================================
# ポリシー設定
# =============================================================================

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

# サービススコープが必要なテーブル（service_id でフィルタ必須）
# 特定のサービスのデータのみ見せたい場合に使用
SERVICE_SCOPED_TABLES: set[str] = set()  # 例: {"campaigns", "campaign_daily_stats"}


# =============================================================================
# 正規表現パターン
# =============================================================================

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

# service_idフィルタの検出
SERVICE_ID_PLACEHOLDER_RE = re.compile(
    r"\b(?:\w+\.)?service_id\s*=\s*:service_id\b",
    re.I,
)
SERVICE_ID_NUMERIC_RE = re.compile(
    r"\b(?:\w+\.)?service_id\s*=\s*\d+\b",
    re.I,
)
SERVICE_ID_IN_RE = re.compile(
    r"\b(?:\w+\.)?service_id\s+IN\s*\(",
    re.I,
)


# =============================================================================
# ヘルパー関数
# =============================================================================


def _normalize_identifier(name: str) -> str:
    """テーブル名を正規化（引用符除去、小文字化）"""
    return re.sub(r'^[`"\[]|[`"\]]$', "", name).lower()


def _extract_tables(query: str) -> tuple[set[str], dict[str, str]]:
    """
    クエリからテーブル名とエイリアスを抽出

    Args:
        query: SQLクエリ

    Returns:
        tuple: (テーブル名のセット, {エイリアス: テーブル名} の辞書)
    """
    tables = set()
    alias_map = {}

    for pattern in [FROM_RE, JOIN_RE]:
        for match in pattern.finditer(query):
            table = _normalize_identifier(match.group(1))
            alias = (match.group(2) or "").lower()
            tables.add(table)
            if alias:
                alias_map[alias] = table

    return tables, alias_map


def _extract_limit(query: str) -> int | None:
    """LIMIT値を抽出"""
    match = HAS_LIMIT_RE.search(query)
    if match:
        return int(match.group(1))
    return None


# =============================================================================
# メインのチェック関数
# =============================================================================


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


def check_query(
    query: str,
    service_id: int | None = None,
    allow_subqueries: bool = False,
    max_limit: int | None = None,
) -> QueryCheckResult:
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
    tables, alias_map = _extract_tables(query)

    if not tables:
        return QueryCheckResult(False, error="テーブル名を特定できませんでした")

    # 許可されていないテーブルへのアクセスチェック
    disallowed = tables - ALLOWED_TABLES
    if disallowed:
        return QueryCheckResult(
            False,
            error=f"アクセスが許可されていないテーブル: {', '.join(sorted(disallowed))}",
        )

    # 9. サービススコープのチェック（設定されている場合）
    if SERVICE_SCOPED_TABLES and service_id is not None:
        scoped_tables = tables & SERVICE_SCOPED_TABLES
        if scoped_tables:
            # 数値リテラルでのservice_id指定は禁止
            if SERVICE_ID_NUMERIC_RE.search(query) or SERVICE_ID_IN_RE.search(query):
                return QueryCheckResult(
                    False,
                    error="service_idには:service_idプレースホルダを使用してください",
                )

            # service_id = :service_id が必須
            if not SERVICE_ID_PLACEHOLDER_RE.search(query):
                return QueryCheckResult(
                    False,
                    error=f"テーブル {', '.join(scoped_tables)} へのアクセスには "
                    f"service_id = :service_id の条件が必要です",
                )

    # 10. LIMIT句の処理
    current_limit = _extract_limit(query)

    if current_limit is None:
        # LIMITがない場合はデフォルトを追加
        query = f"{query} LIMIT {settings.default_limit}"
    elif current_limit > max_limit:
        # 最大値を超えている場合は制限
        query = HAS_LIMIT_RE.sub(f"LIMIT {max_limit}", query)

    return QueryCheckResult(True, query=query)


# =============================================================================
# 便利な関数
# =============================================================================


def safe_query(query: str, **kwargs) -> str:
    """
    クエリをチェックして、安全なクエリを返す
    エラーの場合は例外を発生

    Args:
        query: チェックするSQLクエリ
        **kwargs: check_query に渡す追加引数

    Returns:
        str: 安全なクエリ

    Raises:
        ValueError: クエリが安全でない場合
    """
    result = check_query(query, **kwargs)
    if not result.is_valid:
        raise ValueError(result.error)
    return result.query


def is_safe_query(query: str, **kwargs) -> bool:
    """
    クエリが安全かどうかを判定

    Args:
        query: チェックするSQLクエリ
        **kwargs: check_query に渡す追加引数

    Returns:
        bool: 安全な場合True
    """
    return check_query(query, **kwargs).is_valid
