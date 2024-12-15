-- hw
-- CREATE TYPE film AS (
-- 	name TEXT,
-- 	votes INTEGER,
-- 	rating REAL,
-- 	id TEXT
-- );

-- CREATE TYPE quality_class as ENUM('star', 'good', 'average', 'bad');
DROP TABLE actors;
CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films film[],
	quality_class quality_class,
	is_active BOOLEAN
);

-- SELECT * FROM actor_films

WITH last_year AS (
	SELECT * FROM actors
),
	current_year AS (
	SELECT 
		*
	FROM actor_films
	WHERE year = 1983
	)

SELECT 
	DISTINCT
	COALESCE(ly.actor, cy.actor) as actor,
	COALESCE(ly.actorid, cy.actorid) as actorid,
	CASE WHEN ly.films is NULL
		THEN ARRAY_AGG(ROW(
			cy.film,
			cy.votes,
			cy.rating,
			cy.filmid
		)::film) OVER (PARTITION BY cy.actorid)
		WHEN cy.year IS NOT NULL 
		THEN ly.films || ARRAY[ROW(
			cy.film,
			cy.votes,
			cy.rating,
			cy.filmid
		)::film]
		ELSE ly.films
	END as films,

	-- quality rating
	CASE
		WHEN cy.year IS NOT NULL THEN
			CASE WHEN AVG(cy.rating) OVER (PARTITION BY cy.actorid) > 8 THEN 'star'
			WHEN AVG(cy.rating) OVER (PARTITION BY cy.actorid) > 7 THEN 'good'
			WHEN AVG(cy.rating) OVER (PARTITION BY cy.actorid) > 6 THEN 'average'
			ELSE 'bad'
			END::quality_class
		ELSE ly.quality_class
	END as quality_class,

	-- is active
	CASE 
		WHEN cy.year IS NOT NULL THEN True
		ELSE False
	END as is_active
FROM current_year cy FULL OUTER JOIN last_year ly
ON ly.actorid = cy.actorid;






