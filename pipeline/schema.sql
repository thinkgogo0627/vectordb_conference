-- 리플레이 대상 테이블 (T-04가 INSERT, Debezium이 CDC)
-- content는 Postgres에만 남기고 Kafka 이벤트에서는 제외한다 (connector.json column.exclude.list).
CREATE TABLE IF NOT EXISTS wiki_revisions (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT        NOT NULL,
    page_id       BIGINT      NOT NULL,
    rev_id        BIGINT      NOT NULL,
    parent_rev_id BIGINT,
    page_title    TEXT,
    event_dt      TIMESTAMPTZ NOT NULL,          -- 원래 편집 시각
    content       TEXT,
    t0_host       DOUBLE PRECISION NOT NULL,     -- 리플레이어가 INSERT 직전 찍은 호스트 시각(epoch s). 평가의 기준 시각
    replayed_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    is_base       BOOLEAN NOT NULL DEFAULT false   -- 실험 1: 리플레이 직전 상태 (재지 않음)
);
ALTER TABLE wiki_revisions ADD COLUMN IF NOT EXISTS is_base BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS wiki_revisions_page_rev ON wiki_revisions (page_id, rev_id);

-- 최신 리비전 조회용 (GR-02: 정답 기준은 Postgres)
CREATE OR REPLACE VIEW wiki_latest AS
SELECT DISTINCT ON (run_id, page_id) run_id, page_id, rev_id, id
FROM wiki_revisions
ORDER BY run_id, page_id, rev_id DESC;
