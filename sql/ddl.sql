CREATE DATABASE IF NOT EXISTS llm_ad_agent
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE llm_ad_agent;

-- サービス/プロダクト（何を宣伝しているか）
CREATE TABLE services (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Google広告アカウント
CREATE TABLE ad_accounts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    google_account_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- キャンペーン
CREATE TABLE campaigns (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    google_campaign_id VARCHAR(50) NOT NULL,
    account_id BIGINT NOT NULL,
    service_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    campaign_type ENUM('SEARCH', 'DISPLAY', 'VIDEO', 'SHOPPING', 'APP', 'PERFORMANCE_MAX') NOT NULL,
    status ENUM('ENABLED', 'PAUSED', 'REMOVED') NOT NULL,
    budget_amount DECIMAL(15, 2),
    start_date DATE,
    end_date DATE,
    FOREIGN KEY (account_id) REFERENCES ad_accounts(id),
    FOREIGN KEY (service_id) REFERENCES services(id),
    UNIQUE KEY (google_campaign_id, account_id)
);

-- 広告グループ
CREATE TABLE ad_groups (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    google_adgroup_id VARCHAR(50) NOT NULL,
    campaign_id BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    status ENUM('ENABLED', 'PAUSED', 'REMOVED') NOT NULL,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
    UNIQUE KEY (google_adgroup_id, campaign_id)
);

-- キーワード（リスティング用）
CREATE TABLE keywords (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    google_keyword_id VARCHAR(50),
    ad_group_id BIGINT NOT NULL,
    keyword_text VARCHAR(500) NOT NULL,
    match_type ENUM('EXACT', 'PHRASE', 'BROAD') NOT NULL,
    status ENUM('ENABLED', 'PAUSED', 'REMOVED') NOT NULL,
    FOREIGN KEY (ad_group_id) REFERENCES ad_groups(id),
    UNIQUE KEY (google_keyword_id, ad_group_id)
);

-- 広告クリエイティブ
CREATE TABLE ads (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    google_ad_id VARCHAR(50),
    ad_group_id BIGINT NOT NULL,
    ad_type ENUM('RESPONSIVE_SEARCH', 'RESPONSIVE_DISPLAY', 'IMAGE', 'VIDEO') NOT NULL, -- 広告のタイプ（検索かディスプレイかなど）
    headlines JSON,
    descriptions JSON,
    final_url VARCHAR(2048), -- 出稿された広告のURL
    status ENUM('ENABLED', 'PAUSED', 'REMOVED') NOT NULL,
    FOREIGN KEY (ad_group_id) REFERENCES ad_groups(id),
    UNIQUE KEY (google_ad_id, ad_group_id)
);

-- ターゲティング設定（ディスプレイ/動画用）
CREATE TABLE targeting_settings (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ad_group_id BIGINT NOT NULL,
    targeting_type ENUM('AUDIENCE', 'LOCATION', 'DEVICE', 'AGE', 'GENDER', 'PLACEMENT') NOT NULL,
    targeting_value VARCHAR(500) NOT NULL,
    bid_modifier DECIMAL(5, 2),
    FOREIGN KEY (ad_group_id) REFERENCES ad_groups(id)
);

-- 検索クエリ（ユーザーが実際に検索した語句）
CREATE TABLE search_queries (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    query_text VARCHAR(500) NOT NULL,
    UNIQUE KEY (query_text)
);

-- リスティング用：検索クエリ × キーワード × 広告 の掛け合わせ日次実績
CREATE TABLE search_query_keyword_ad_daily_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    search_query_id BIGINT NOT NULL,
    keyword_id BIGINT NOT NULL,
    ad_id BIGINT NOT NULL,
    date DATE NOT NULL,
    impressions INT DEFAULT 0,
    clicks INT DEFAULT 0,
    cost DECIMAL(15, 2) DEFAULT 0,
    conversions DECIMAL(10, 2) DEFAULT 0,
    conversion_value DECIMAL(15, 2) DEFAULT 0,
    FOREIGN KEY (search_query_id) REFERENCES search_queries(id),
    FOREIGN KEY (keyword_id) REFERENCES keywords(id),
    FOREIGN KEY (ad_id) REFERENCES ads(id),
    UNIQUE KEY (search_query_id, keyword_id, ad_id, date),
    INDEX idx_date (date),
    INDEX idx_query (search_query_id),
    INDEX idx_keyword (keyword_id),
    INDEX idx_ad (ad_id)
);

-- ディスプレイ/動画用：広告単体の日次実績
CREATE TABLE display_ad_daily_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    ad_id BIGINT NOT NULL,
    date DATE NOT NULL,
    impressions INT DEFAULT 0,
    clicks INT DEFAULT 0,
    cost DECIMAL(15, 2) DEFAULT 0,
    conversions DECIMAL(10, 2) DEFAULT 0,
    conversion_value DECIMAL(15, 2) DEFAULT 0,
    FOREIGN KEY (ad_id) REFERENCES ads(id),
    UNIQUE KEY (ad_id, date),
    INDEX idx_date (date)
);

-- キャンペーン日次サマリー（全体集計用）
CREATE TABLE campaign_daily_stats (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    campaign_id BIGINT NOT NULL,
    date DATE NOT NULL,
    impressions INT DEFAULT 0,
    clicks INT DEFAULT 0,
    cost DECIMAL(15, 2) DEFAULT 0,
    conversions DECIMAL(10, 2) DEFAULT 0,
    conversion_value DECIMAL(15, 2) DEFAULT 0,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
    UNIQUE KEY (campaign_id, date),
    INDEX idx_date (date)
);