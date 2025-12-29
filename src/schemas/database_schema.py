SCHEMA_INFO = """
## データベーススキーマ

### テーブル階層とリレーション

services
    └── campaigns (service_id → services.id)
            └── ad_groups (campaign_id → campaigns.id)
                    ├── keywords (ad_group_id → ad_groups.id)
                    ├── ads (ad_group_id → ad_groups.id)
                    └── targeting_settings (ad_group_id → ad_groups.id)

ad_accounts
    └── campaigns (account_id → ad_accounts.id)

search_queries
    └── search_query_keyword_ad_daily_stats (search_query_id → search_queries.id)

【実績テーブルのリレーション】
- search_query_keyword_ad_daily_stats: keyword_id → keywords.id, ad_id → ads.id
- display_ad_daily_stats: ad_id → ads.id
- campaign_daily_stats: campaign_id → campaigns.id

### 実績テーブルの選択ルール

集計したい単位に応じて適切な実績テーブルを選ぶこと：

| 集計単位 | 使用テーブル |
|----------|-------------|
| サービス単位 | campaign_daily_stats |
| アカウント単位 | campaign_daily_stats |
| キャンペーン単位 | campaign_daily_stats |
| 広告グループ単位 | ads経由でsearch_query_keyword_ad_daily_statsまたはdisplay_ad_daily_stats |
| キーワード単位 | search_query_keyword_ad_daily_stats |
| 広告単位 | search_query_keyword_ad_daily_statsまたはdisplay_ad_daily_stats |
| 検索クエリ単位 | search_query_keyword_ad_daily_stats |

広告タイプによる選択：
- 全広告タイプを含む集計 → campaign_daily_stats
- 検索広告のみ → search_query_keyword_ad_daily_stats
- ディスプレイ広告のみ → display_ad_daily_stats

### JOINの原則

1. **階層をたどる**: 直接JOINせず、テーブル階層に従って中間テーブルを経由する
2. **上位階層はcampaign_daily_statsを使う**: services, ad_accounts, campaigns レベルの集計はcampaign_daily_statsが効率的
3. **外部キーを確認**: ad_id → ads.id, keyword_id → keywords.id, campaign_id → campaigns.id

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
- match_type: ENUM('EXACT', 'PHRASE', 'BROAD'), status

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

**campaign_daily_stats** - キャンペーン実績（全広告タイプ含む）
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
