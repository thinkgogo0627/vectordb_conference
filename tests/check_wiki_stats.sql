-- 위키 수집 데이터 점검: 실험 2와 인덱스 아이디어 1·2를 할 만한지 판단 (plan.md 4·5·10절)
--   ① 되돌림이 충분한가        → 2, 4번  (적으면 실험 2 위키 쪽 균형점이 약함)
--   ② 수상한 편집을 가려낼 수 있나 → 4-2번 (적응형 정책 근거)
--   ③ 늦추면 무엇을 얻고 잃나    → 5번   (고정 지연별 건너뛰는 쓰기 / 잘못된 노출 / 늦은 손해)
--   ④ 편집이 몰리나             → 6번   (아이디어 2 전제: 상위 1% 문서가 편집의 20~30% 이상)
-- DuckDB CLI에서 실행:  duckdb -c ".read check_wiki_stats.sql"   (또는 duckdb 실행 후 .read)
-- 0번 블록의 키 3개와 경로만 바꾸면 됨. 시간은 전부 rc_timestamp(위키 기준 변경 시각, 초) 기준.

-- 0. 접속 ------------------------------------------------------------------
INSTALL httpfs; LOAD httpfs;
CREATE OR REPLACE SECRET r2 (TYPE R2, KEY_ID '<읽기전용키>', SECRET '<시크릿>', ACCOUNT_ID '<계정ID>');

-- 1. 편집만 모으고 중복 제거 (본문은 안 읽어서 빠름) ----------------------------
--    편집자 이름은 원본 JSON에서만 꺼냄. editor = bot / anon(IP·임시계정) / user
CREATE OR REPLACE TABLE ev AS
WITH raw AS (
    SELECT * EXCLUDE (content, raw_event),
           json_extract_string(raw_event, '$.user') AS user_name
    FROM read_parquet('r2://wiki-lake/raw/wiki_recentchange/source=kowiki/date=*/*/*.parquet',
                      hive_partitioning = true, union_by_name = true)
    WHERE op IN ('create', 'update')
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingested_at) = 1
)
SELECT *,
       CASE WHEN is_bot THEN 'bot'
            WHEN user_name LIKE '~%'                                         -- 임시계정 (~2025-…)
              OR regexp_matches(user_name, '^\d{1,3}(\.\d{1,3}){3}$')        -- IPv4
              OR user_name LIKE '%:%:%'                                      -- IPv6
            THEN 'anon'
            ELSE 'user' END AS editor
FROM raw;

-- 2. 기본 현황 --------------------------------------------------------------
--    sha1_coverage가 낮으면(본문 조회 실패) 되돌림 판별이 덜 잡힘
SELECT
    count(*)                                         AS edits,
    count(DISTINCT page_title)                       AS pages,
    min(event_dt)                                    AS first_event,
    max(event_dt)                                    AS last_event,
    round((max(rc_timestamp) - min(rc_timestamp)) / 3600.0, 1) AS hours_covered,
    round(100 * avg(is_bot::INT), 1)                 AS bot_pct,
    round(100 * avg((editor = 'anon')::INT), 1)      AS anon_pct,
    round(100 * avg((sha1 IS NOT NULL)::INT), 1)     AS sha1_coverage_pct,
    round(100 * avg(list_has_any(coalesce(tags, []),
          ['mw-rollback', 'mw-undo', 'mw-manual-revert'])::INT), 2) AS revert_tag_pct
FROM ev;

-- 3. 되돌려진 편집 찾기 ------------------------------------------------------
--    같은 문서에서 24시간 안에 예전 버전과 본문(sha1)이 똑같아진 편집 = 되돌림 (reverts)
--    그 사이에 있던 편집들 = 되돌려진 편집 (reverted, 장난·실수 후보)
CREATE OR REPLACE TABLE reverts AS     -- (복원된 버전, 되돌림 편집)
WITH r AS (
    SELECT page_title, rev_id, rc_timestamp AS ts, sha1
    FROM ev WHERE sha1 IS NOT NULL
)
SELECT b.page_title, b.rev_id AS revert_rev, b.ts AS revert_ts,
       max(a.rev_id) AS restored_rev
FROM r a
JOIN r b ON a.page_title = b.page_title
        AND a.sha1 = b.sha1
        AND a.rev_id < b.rev_id
        AND b.ts - a.ts <= 86400
GROUP BY ALL;

CREATE OR REPLACE TABLE reverted AS
SELECT e.page_title, e.rev_id, e.rc_timestamp AS ts, e.is_bot, e.editor,
       p.revert_rev, p.revert_ts, p.revert_ts - e.rc_timestamp AS sec_to_revert
FROM ev e
JOIN reverts p ON e.page_title = p.page_title
              AND e.rev_id > p.restored_rev
              AND e.rev_id < p.revert_rev
QUALIFY row_number() OVER (PARTITION BY e.rev_id ORDER BY p.revert_ts) = 1;

-- 4. 되돌림 비율과 걸린 시간 (실험 2 대기 시간 후보의 근거) ---------------------
SELECT
    (SELECT count(*) FROM ev)                                   AS edits,
    count(*)                                                    AS reverted_edits,
    round(100.0 * count(*) / (SELECT count(*) FROM ev), 2)      AS reverted_pct,
    round(100.0 * count(*) FILTER (WHERE NOT is_bot)
          / (SELECT count(*) FROM ev WHERE NOT is_bot), 2)      AS reverted_pct_human,
    quantile_cont(sec_to_revert, 0.5)                           AS median_sec,
    quantile_cont(sec_to_revert, 0.9)                           AS p90_sec
FROM reverted;

--    sha1 판별이 되돌림 태그를 얼마나 잡나. 낮으면 수집 시작 전 버전으로 되돌린 경우를
--    놓치고 있다는 뜻 → 위 비율은 하한값으로 봐야 함
SELECT
    count(*)                                         AS tagged_reverts,
    count(p.revert_rev)                              AS also_found_by_sha1,
    round(100.0 * count(p.revert_rev) / count(*), 1) AS sha1_recall_pct,
    (SELECT count(*) FROM reverts)                   AS sha1_reverts_total
FROM ev e LEFT JOIN reverts p ON e.rev_id = p.revert_rev
WHERE list_has_any(coalesce(e.tags, []), ['mw-rollback', 'mw-undo', 'mw-manual-revert']);

SELECT
    CASE WHEN sec_to_revert <    60 THEN '1) 1분 안'
         WHEN sec_to_revert <   300 THEN '2) 1~5분'
         WHEN sec_to_revert <   900 THEN '3) 5~15분'
         WHEN sec_to_revert <  3600 THEN '4) 15~60분'
         ELSE                            '5) 1~24시간' END       AS time_to_revert,
    count(*)                                                     AS n,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)           AS pct
FROM reverted
GROUP BY 1 ORDER BY 1;

-- 4-2. 누가·어떤 편집이 되돌려지나 (적응형 정책: 규칙으로 위험 편집을 가려낼 수 있나) -----
--      그룹 간 되돌림 비율 차이가 클수록 "수상한 것만 늦추기"가 먹힘
WITH x AS (
    SELECT e.*, rv.rev_id IS NOT NULL AS was_reverted
    FROM ev e LEFT JOIN reverted rv ON e.rev_id = rv.rev_id
)
SELECT 'editor'    AS rule, editor AS grp,
       count(*) AS edits, count(*) FILTER (WHERE was_reverted) AS reverted,
       round(100 * avg(was_reverted::INT), 2) AS reverted_pct
FROM x GROUP BY ALL
UNION ALL
SELECT 'size_change', CASE WHEN length_new < length_old * 0.7 THEN '30%+ 삭제'
                           WHEN length_new > length_old * 1.3 THEN '30%+ 추가'
                           ELSE '그 외' END,
       count(*), count(*) FILTER (WHERE was_reverted),
       round(100 * avg(was_reverted::INT), 2)
FROM x WHERE NOT is_bot GROUP BY ALL
ORDER BY rule, grp;

-- 5. 고정 지연 정책 미리보기 (실험 2 + 아이디어 1) -------------------------------
--    정책: 편집마다 d초 들고 있다가, 그 사이 같은 문서에 새 버전이 오면 옛 것은 안 씀
--    skipped_write_pct     : 벡터DB에 안 써도 되는 편집 비율 (인덱스 휘젓기 감소)
--    bad_never_shown_pct   : 되돌려진 편집 중 한 번도 노출 안 된 비율
--    bad_exposure_min / pct: 되돌려진 편집이 검색에 보인 총 시간 (즉시 반영 대비 %)  = 잘못된 노출
--    normal_stale_avg_sec  : 정상 편집 하나가 반영 안 된 채 기다린 평균 시간       = 늦은 손해
--    (파이프라인 지연은 0으로 가정. 실험 1 지연 분포를 더하면 실험 2 시뮬레이션이 됨)
WITH n AS (
    SELECT e.page_title, e.rev_id, e.is_bot, e.rc_timestamp AS ts,
           lead(e.rc_timestamp) OVER (PARTITION BY e.page_title ORDER BY e.rev_id)
               - e.rc_timestamp                                AS gap,   -- 다음 편집까지 (없으면 NULL)
           rv.rev_id IS NOT NULL                               AS bad
    FROM ev e LEFT JOIN reverted rv ON e.rev_id = rv.rev_id
),
s AS (
    SELECT t.d, n.*, coalesce(gap <= t.d, false) AS skipped
    FROM n, (VALUES (0), (60), (300), (900), (3600)) t(d)
),
w AS (              -- 실제로 써지는 편집: 다음에 써지는 편집이 올 때까지 보임
    SELECT d, bad,
           lead(ts) OVER (PARTITION BY d, page_title ORDER BY rev_id) - ts AS visible_sec
    FROM s WHERE NOT skipped
),
exposure AS (
    SELECT d, sum(visible_sec) FILTER (WHERE bad) AS bad_sec FROM w GROUP BY d
)
SELECT s.d                                                                   AS delay_sec,
       round(100 * avg(skipped::INT), 1)                                     AS skipped_write_pct,
       round(100 * avg(skipped::INT) FILTER (WHERE NOT is_bot), 1)           AS skipped_write_pct_human,
       round(100 * avg(skipped::INT) FILTER (WHERE bad), 1)                  AS bad_never_shown_pct,
       round(any_value(x.bad_sec) / 60.0)                                    AS bad_exposure_min,
       round(100.0 * any_value(x.bad_sec)
             / max(any_value(x.bad_sec)) OVER (), 1)                         AS bad_exposure_pct,
       round(avg(least(s.d, coalesce(gap, s.d))) FILTER (WHERE NOT bad), 1)  AS normal_stale_avg_sec
FROM s JOIN exposure x ON s.d = x.d
GROUP BY s.d ORDER BY s.d;

-- 6. 편집 쏠림 (아이디어 2: 자주 바뀌는 문서만 따로 둘 가치가 있나) ---------------
--    봇이 문서마다 한 번씩 훑고 지나가면 쏠림이 묽어 보여서 사람 편집만 따로도 봄
WITH c AS (
    SELECT 'all'   AS scope, page_title, count(*) AS n FROM ev GROUP BY ALL
    UNION ALL
    SELECT 'human' AS scope, page_title, count(*) AS n FROM ev WHERE NOT is_bot GROUP BY ALL
),
k AS (
    SELECT scope, n,
           row_number() OVER (PARTITION BY scope ORDER BY n DESC) AS rk,
           count(*)     OVER (PARTITION BY scope)                 AS pages,
           sum(n)       OVER (PARTITION BY scope)                 AS total
    FROM c
)
SELECT
    scope,
    max(pages)                                                            AS pages,
    max(total)                                                            AS edits,
    round(100.0 * sum(n) FILTER (WHERE rk <= ceil(pages * 0.01)) / max(total), 1) AS top1pct_pages_share,
    round(100.0 * sum(n) FILTER (WHERE rk <= ceil(pages * 0.10)) / max(total), 1) AS top10pct_pages_share,
    round(100.0 * count(*) FILTER (WHERE n = 1) / max(pages), 1)          AS edited_once_pct
FROM k
GROUP BY scope ORDER BY scope;

-- 많이 고쳐진 문서 상위 20개 (봇 비율, 되돌려진 편집 수 같이)
SELECT e.page_title,
       count(*)                          AS edits,
       round(100 * avg(e.is_bot::INT))   AS bot_pct,
       count(rv.rev_id)                  AS reverted
FROM ev e LEFT JOIN reverted rv ON e.rev_id = rv.rev_id
GROUP BY 1 ORDER BY edits DESC LIMIT 20;
