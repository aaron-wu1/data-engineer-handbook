-- fact modeling lab 2

-- ddl---------
-- CREATE TABLE users_cumulated (
-- 	user_id TEXT,
-- 	-- list of dates in the past where user was active
-- 	dates_active DATE[],
-- 	-- current date for the user
-- 	date DATE,
-- 	PRIMARY KEY(user_id, date)
-- )


-- cumulative data add date
INSERT INTO users_cumulated
WITH yesterday as(
	SELECT 
		*
	FROM users_cumulated
	WHERE date = DATE('2023-01-30')
),
	today AS (
		SELECT 
			CAST(user_id AS TEXT),
			DATE(CAST(event_time AS TIMESTAMP)) AS date_active
		FROM events
		WHERE 
			DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
			AND user_id IS NOT NULL
		GROUP BY
			user_id, DATE(CAST(event_time AS TIMESTAMP))
	)
SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	-- aggs dates
	CASE WHEN y.dates_active IS NULL
		THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.dates_active
		ELSE y.dates_active || ARRAY[t.date_active]
		END
		as dates_active,
	-- plus one because on init yesterday's date is off by 1 day
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t 
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id;

-- turn active days into bitmask
-- active days in the month from 01/01/23 - 01/31/23
-- check is active within last month, week
WITH users AS (
	SELECT
		*
	FROM users_cumulated
	WHERE date = DATE('2023-01-31')
), 
	series AS (
		-- generate serieces of dates
	SELECT * 
	FROM 
		generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')
		AS series_date
	),
	place_holder_ints AS (
		SELECT
			CASE WHEN
				dates_active @> ARRAY[DATE(series_date)]
			THEN 
				CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
					ELSE 0
			END as place_holder_int_value,
			*
		FROM users CROSS JOIN series
	)
SELECT 
	user_id,
	SUM(place_holder_int_value),
	CAST(CAST(SUM(place_holder_int_value)AS BIGINT) AS BIT(32)),
	BIT_COUNT(CAST(CAST(SUM(place_holder_int_value)AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
	-- bitwise and with last weekly dates
	BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &
	CAST(CAST(SUM(place_holder_int_value)AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active
	
FROM place_holder_ints
GROUP BY user_id

-- SELECT DISTINCT date FROM users_cumulated
-- ORDER BY date
-- WHERE date = DATE('2023-01-31')



