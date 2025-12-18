"""
データベーススキーマ情報
LLMへのプロンプト用スキーマ定義
"""

SCHEMA_INFO = """
## データベーススキーマ

### マスターデータ

**services** - サービス（事業）
- id, name, description, created_at

**ad_accounts** - 広告アカウント
- id, google_account_id, name, created_at

**campaigns** - キャンペーン
- id, google_campaign_id, account_id, service_id, name
- campaign_type: ENUM('SEARCH', 'DISPLAY', 'VIDEO', 'SHOPPING', 'APP', 'PERFORMANCE_MAX')
- status: ENUM('ENABLED', 'PAUSED', 'REMOVED')
- budget_amount, start_date, end_date

**ad_groups** - 広告グループ
- id, google_adgroup_id, campaign_id, name, status

**keywords** - キーワード
- id, google_keyword_id, ad_group_id, keyword_text
- match_type: ENUM('EXACT', 'PHRASE', 'BROAD')
- status

**ads** - 広告
- id, google_ad_id, ad_group_id
- ad_type: ENUM('RESPONSIVE_SEARCH', 'RESPONSIVE_DISPLAY', 'IMAGE', 'VIDEO')
- headlines(JSON), descriptions(JSON), final_url, status

**search_queries** - 検索クエリ
- id, query_text

### 実績データ（日別統計）

**search_query_keyword_ad_daily_stats** - 検索広告実績
- search_query_id, keyword_id, ad_id, date
- impressions, clicks, cost, conversions, conversion_value

**display_ad_daily_stats** - ディスプレイ広告実績
- ad_id, date
- impressions, clicks, cost, conversions, conversion_value

**campaign_daily_stats** - キャンペーン実績
- campaign_id, date
- impressions, clicks, cost, conversions, conversion_value

### 主要指標の計算式
- CTR = clicks / impressions * 100
- CPC = cost / clicks
- CVR = conversions / clicks * 100
- CPA = cost / conversions
- ROAS = conversion_value / cost * 100

### データ期間
2020-01-01 〜 2024-12-31

### サービス一覧
1. ECサイトA  2. SaaSプロダクトB  3. 転職サービスC
"""
