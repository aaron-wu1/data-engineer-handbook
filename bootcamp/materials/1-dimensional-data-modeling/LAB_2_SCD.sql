-- creates SCD table (type2)
-- CREATE TABLE players_scd (
-- 	player_name TEXT,
-- 	-- tracking columns in history
-- 	scoring_class scoring_class,
-- 	is_active BOOLEAN,
-- 	-- range for SCD
-- 	start_season INTEGER,
-- 	end_season INTEGER,
-- 	current_season INTEGER,
-- 	PRIMARY KEY(player_name, start_season)
-- );

-- DROP TABLE players_scd

-- adding to players
-- INSERT INTO players_scd
-- CTE
WITH with_previous AS (
	SELECT  
		player_name, 
		current_season,
		scoring_class,
		is_active,
		LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
		LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
	FROM players
	WHERE current_season <= 2021
),
	with_indicators AS (
		-- adds a boolean to check if scoring_class and is_active has changed
		SELECT *, 
			CASE 
				WHEN scoring_class <> previous_scoring_class THEN 1 
				WHEN is_active <> previous_is_active THEN 1 
				ELSE 0 
			END AS change_indicator
		FROM with_previous
	),
	with_streaks AS (
		-- adds streaks of non-changes
		SELECT 
			*, 
			SUM(change_indicator) 
				OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
		FROM with_indicators
	)

-- query for checking contents of above CTE
-- SELECT 
-- 	player_name,
-- 	scoring_class,
-- 	is_active,
-- 	MIN(current_season) as start_season,
-- 	MAX(current_season) as end_season,
-- 	-- hardcode
-- 	2021 AS current_season
-- FROM with_streaks
-- GROUP BY player_name, streak_identifier, is_active, scoring_class
-- ORDER BY player_name, streak_identifier

-- SELECT * FROM players_scd;

-- NOTE: EXPENSIVE PART OF QUERY is the window functions, (eg. OVER)
-- window fn over entire dataset and only until the group by we strink the data
-- prone to skew or running out of memory (despite being a SCD, some people might be actively changing)
-- blowing up the cardinality of dataset

-- create scd type for changed records column
-- CREATE TYPE scd_type AS (
-- 	scoring_class scoring_class,
-- 	is_active boolean,
-- 	start_season INTEGER,
-- 	end_season INTEGER
-- )


WITH last_season_scd AS (
	SELECT * FROM players_scd
	WHERE current_season = 2021
	AND end_season =  2021
),
	-- unchangable, already happened
	historical_scd AS (
		SELECT
			player_name,
			scoring_class,
			is_active,
			start_season,
			end_season
		FROM players_scd
		WHERE current_season = 2021
		AND end_season < 2021
	),
	this_season_data AS (
		SELECT * FROM players
		WHERE current_season = 2022
	),
	-- for records that don't change from last season, 
	-- end season set to incremented by 1
	unchanged_records AS (
		SELECT 
			ts.player_name,
			ts.scoring_class, 
			ts.is_active,
			ls.start_season, 
			ts.current_season as end_season
		FROM this_season_data ts
		JOIN last_season_scd ls
		ON ls.player_name = ts.player_name
		WHERE ts.scoring_class = ls.scoring_class
		AND ts.is_active = ls.is_active
	),
	changed_records AS (
		SELECT 
			ts.player_name,
			-- explode out slowly changing dimension data
			UNNEST(ARRAY[
				ROW(
					ls.scoring_class,
					ls.is_active,
					ls.start_season,
					ls.end_season
				)::scd_type,
				ROW(
					ts.scoring_class,
					ts.is_active,
					ts.current_season,
					ts.current_season
				)::scd_type
			]) as records
		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
		ON ls.player_name = ts.player_name
		-- if changed
		WHERE ts.scoring_class <> ls.scoring_class
		OR ts.is_active <> ls.is_active
	),
	unnested_changed_records AS (
		SELECT 
			player_name,
			-- flatten scd
			(records::scd_type).scoring_class,
			(records::scd_type).is_active,
			(records::scd_type).start_season,
			(records::scd_type).end_season
		FROM changed_records
	),
	new_records AS (
		SELECT 
			ts.player_name,
			ts.scoring_class,
			ts.is_active,
			ts.current_season AS start_season,
			ts.current_season as end_season
		FROM this_season_data ts
		LEFT JOIN last_season_scd ls
			ON ts.player_name = ls.player_name
		WHERE ls.player_name IS NULL
	)
	
-- QUERY Changed or Unchanged records
-- SELECT * FROM unchanged_records
-- SELECT * FROM changed_records
-- exploded out changed scds
-- SELECT * FROM unnested_changed_records

-- QUERY processes about 20x times less data
-- takes historical data and appends new data if changed
-- NOTE: assumed that is_active and scoring class is never null
-- breaks pattern on the WHERE (eg. null != null -> gets filtered out)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records

-- with new players, left join, template for unchanged records
-- SELECT ts.player_name,
-- 		ts.scoring_class, ts.is_active,
-- 		ls.scoring_class, ls.is_active
-- 	FROM this_season_data ls
-- 	LEFT JOIN last_season_scd ts
-- 	ON ls.player_name = ts.player_name





-- /* 
-- BELOW PART 1 Updating table
-- Showcases the power of storing temporal data into a struct

-- USECASES: useful in monitoring, analytics, aggergation
-- PROS: can be distributed, no aggergation or group by when querying
-- */ 

-- --SELECT * FROM player_seasons;
-- -- add one row per player, array per season
-- -- push temporal component in it's own data struct (season)

-- -- data type struct for season,stats
-- --CREATE TYPE season_stats AS (
-- --        season INTEGER,
-- --        gp INTEGER,
-- --        pts REAL,
-- --        reb REAL,
-- --        ast REAL
-- --        );

-- --CREATE TYPE scoring_class as ENUM ('star', 'good', 'average', 'bad');
-- --
-- ---- building new table columns, static values
-- --CREATE TABLE players (
-- --        player_name TEXT,
-- --        height TEXT,
-- --        college TEXT,
-- --        country TEXT,
-- --        draft_year TEXT,
-- --        draft_round TEXT,
-- --        draft_number TEXT,
-- --        season_stats season_stats[],
-- --        scoring_class scoring_class,
-- --        years_since_last_season INTEGER,
-- --        -- developing table cummulatively
-- --        current_season INTEGER,
-- --        -- unique identifier
-- --        PRIMARY KEY(player_name, current_season)
-- --       );

-- -- find first year
-- -- SELECT MIN(season) FROM player_seasons;

-- --pipe into players
-- INSERT INTO players


-- -- outer join logic, creates temporary table

-- -- seed query, allows addition of temporal data
-- -- if we change the dates, we can add new seasonal data
-- WITH yesterday AS (
--         SELECT * FROM players
--         WHERE current_season = 2000
-- ),
--         today AS (
--                 SELECT * FROM player_seasons
--                 WHERE season = 2001
        
--        )
 
--  -- joins players from both previous season and current season
-- -- removes initial seed nulls
-- SELECT
--         COALESCE(t.player_name, y.player_name) as player_name,
--         COALESCE(t.height, y.height) as height,
--         COALESCE(t.college, y.college) as college,
--         COALESCE(t.country, y.country) as country,
--         COALESCE(t.draft_year, y.draft_year) as draft_year,
--         COALESCE(t.draft_round, y.draft_round) as draft_round,
--         COALESCE(t.draft_number, y.draft_number) as draft_number,
--         -- build array if NULL
--         CASE WHEN y.season_stats is NULL
--                 THEN ARRAY[ROW(
--                         t.season,
--                         t.gp,
--                         t.pts,
--                         t.reb,
--                         t.ast
--                 )::season_stats] -- cast as season stats struct
--         -- if player played this season, concat season stats to array if not NULL 
--         WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
--                         t.season,
--                         t.gp,
--                         t.pts,
--                         t.reb,
--                         t.ast
--                 )::season_stats]
--         -- if player retired, return last season stats
--         ELSE y.season_stats
--         END as season_stats,
--         -- set scoring class when cur season is non null 
--         CASE
--                 WHEN t.season IS NOT NULL THEN 
--                         CASE WHEN t.pts > 20 THEN 'star'
--                         WHEN t.pts > 15 THEN 'good'
--                         WHEN t.pts > 10 THEN 'average'
--                         ELSE 'bad'
--                         END::scoring_class
--                 ELSE y.scoring_class       
--         END as scoring_class,
--         -- if current season is not null then accumate years_since_last_season
--         CASE
--                 WHEN t.season IS NOT NULL THEN 0
--         ELSE y.years_since_last_season + 1
--         END,
--         -- today's stats else return yesterday's season + 1
--         COALESCE(t.season, y.current_season + 1) as current_season
--        FROM today t FULL OUTER JOIN yesterday y
--         ON t.player_name = y.player_name;


-- --SELECT * FROM players 
-- --WHERE current_season = 2001
-- --and player_name = 'Michael Jordan';

-- /*-- returns each season for a player, CTE of season stats
-- -- note: keeps temporal pieces of data together
-- WITH unnested AS (
--         SELECT player_name,
--                 UNNEST(season_stats)::season_stats AS season_stats
--                 FROM players
--         WHERE current_season = 2001
--         and player_name = 'Michael Jordan'
-- )
-- -- returns the original schema (reversal of season stats)
-- -- pass season stats raw data into season stats struct
-- SELECT player_name,
--         (season_stats::season_stats).*
-- FROM unnested
--         */
        
-- --DROP TABLE players

-- -- checks for most improved players point wise from 1 to latest
-- -- example of data anaylsis on data without GROUP BY
-- -- for spark, everything could happen in the map step instead of reduce
-- SELECT 
--         player_name,
--         -- CARDINALITY accesses the last element in the array (latest pts in season
--         (season_stats[CARDINALITY(season_stats)]::season_stats).pts /
--         -- Gets points from first season
--         CASE 
--                 WHEN (season_stats[1]::season_stats).pts = 0
--                 THEN 1
--                 ELSE (season_stats[1]::season_stats).pts
--         END
-- FROM players WHERE current_season = 2001
-- AND scoring_class = 'star'
-- ORDER BY 2 DESC;



