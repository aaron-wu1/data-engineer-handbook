/* 
Showcases the power of storing temporal data into a struct

USECASES: useful in monitoring, analytics, aggergation
PROS: can be distributed, no aggergation or group by when querying
*/ 

--SELECT * FROM player_seasons;
-- add one row per player, array per season
-- push temporal component in it's own data struct (season)

-- data type struct for season,stats
--CREATE TYPE season_stats AS (
--        season INTEGER,
--        gp INTEGER,
--        pts REAL,
--        reb REAL,
--        ast REAL
--        );

--CREATE TYPE scoring_class as ENUM ('star', 'good', 'average', 'bad');
--
---- building new table columns, static values
--CREATE TABLE players (
--        player_name TEXT,
--        height TEXT,
--        college TEXT,
--        country TEXT,
--        draft_year TEXT,
--        draft_round TEXT,
--        draft_number TEXT,
--        season_stats season_stats[],
--        scoring_class scoring_class,
--        years_since_last_season INTEGER,
--        -- developing table cummulatively
--        current_season INTEGER,
--        -- unique identifier
--        PRIMARY KEY(player_name, current_season)
--       );

-- find first year
-- SELECT MIN(season) FROM player_seasons;

--pipe into players
INSERT INTO players


-- outer join logic, creates temporary table

-- seed query, allows addition of temporal data
-- if we change the dates, we can add new seasonal data
WITH yesterday AS (
        SELECT * FROM players
        WHERE current_season = 2000
),
        today AS (
                SELECT * FROM player_seasons
                WHERE season = 2001
        
       )
 
 -- joins players from both previous season and current season
-- removes initial seed nulls
SELECT
        COALESCE(t.player_name, y.player_name) as player_name,
        COALESCE(t.height, y.height) as height,
        COALESCE(t.college, y.college) as college,
        COALESCE(t.country, y.country) as country,
        COALESCE(t.draft_year, y.draft_year) as draft_year,
        COALESCE(t.draft_round, y.draft_round) as draft_round,
        COALESCE(t.draft_number, y.draft_number) as draft_number,
        -- build array if NULL
        CASE WHEN y.season_stats is NULL
                THEN ARRAY[ROW(
                        t.season,
                        t.gp,
                        t.pts,
                        t.reb,
                        t.ast
                )::season_stats] -- cast as season stats struct
        -- if player played this season, concat season stats to array if not NULL 
        WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
                        t.season,
                        t.gp,
                        t.pts,
                        t.reb,
                        t.ast
                )::season_stats]
        -- if player retired, return last season stats
        ELSE y.season_stats
        END as season_stats,
        -- set scoring class when cur season is non null 
        CASE
                WHEN t.season IS NOT NULL THEN 
                        CASE WHEN t.pts > 20 THEN 'star'
                        WHEN t.pts > 15 THEN 'good'
                        WHEN t.pts > 10 THEN 'average'
                        ELSE 'bad'
                        END::scoring_class
                ELSE y.scoring_class       
        END as scoring_class,
        -- if current season is not null then accumate years_since_last_season
        CASE
                WHEN t.season IS NOT NULL THEN 0
        ELSE y.years_since_last_season + 1
        END,
        -- today's stats else return yesterday's season + 1
        COALESCE(t.season, y.current_season + 1) as current_season
       FROM today t FULL OUTER JOIN yesterday y
        ON t.player_name = y.player_name;


--SELECT * FROM players 
--WHERE current_season = 2001
--and player_name = 'Michael Jordan';

/*-- returns each season for a player, CTE of season stats
-- note: keeps temporal pieces of data together
WITH unnested AS (
        SELECT player_name,
                UNNEST(season_stats)::season_stats AS season_stats
                FROM players
        WHERE current_season = 2001
        and player_name = 'Michael Jordan'
)
-- returns the original schema (reversal of season stats)
-- pass season stats raw data into season stats struct
SELECT player_name,
        (season_stats::season_stats).*
FROM unnested
        */
        
--DROP TABLE players

-- checks for most improved players point wise from 1 to latest
-- example of data anaylsis on data without GROUP BY
-- for spark, everything could happen in the map step instead of reduce
SELECT 
        player_name,
        -- CARDINALITY accesses the last element in the array (latest pts in season
        (season_stats[CARDINALITY(season_stats)]::season_stats).pts /
        -- Gets points from first season
        CASE 
                WHEN (season_stats[1]::season_stats).pts = 0
                THEN 1
                ELSE (season_stats[1]::season_stats).pts
        END
FROM players WHERE current_season = 2001
AND scoring_class = 'star'
ORDER BY 2 DESC;



