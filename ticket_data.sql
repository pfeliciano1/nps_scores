-- ALL QUERIES WERE MADE IN POSTGRESQL QUERY TOOL
-- Creating both tables ticket_data201803 and top_15_MIT_AI_2018_01_31
DROP TABLE ticket_data201803;

CREATE TABLE ticket_data201803 (
	ticket_id INTEGER NULL,
	status VARCHAR(40),
	priority VARCHAR(40),
	source VARCHAR(40),
	ticket_group VARCHAR(60),
	created_time DATE,
	resolved_time DATE,
	initial_time DATE,
	agent_interactions INTEGER,
	customer_interactions VARCHAR(40),
	type VARCHAR(60),
	course VARCHAR(60),
	user_id INTEGER,
	agent_id INTEGER
);
-- Verifying data from both tables
SELECT * FROM ticket_data201803;

CREATE TABLE top_15_MIT_AI_2018_01_31 (
	user_id INTEGER
);

SELECT * FROM top_15_MIT_AI_2018_01_31;

-- Record count from both tables
SELECT 
    COUNT(*)
FROM 
    ticket_data201803;
-- Record count: 14339

SELECT
    COUNT(*)
FROM
    top_15_MIT_AI_2018_01_31;
-- Record count: 15

-- Identify if there are any duplicate
SELECT 
    ticket_id
FROM 
    ticket_data201803
GROUP BY 
    ticket_id
HAVING 
    COUNT(ticket_id) > 1;

-- Show the records that have duplicate
SELECT 
    t1.*
FROM 
    ticket_data201803 t1
JOIN 
    (SELECT ticket_id, COUNT(ticket_id)
FROM 
    ticket_data201803
GROUP BY 
    ticket_id
HAVING 
    COUNT(ticket_id) > 1) t2
ON 
    t1.ticket_id = t2.ticket_id
ORDER BY 
    ticket_id;

-- How many duplicate records are there? 228 records
-- Are these true duplicates? 
-- No, some of the same ticket_ids have NULL values in the columns but are different records

-- Remove the duplicate records from ticket_data201803
DELETE FROM ticket_data201803
WHERE ticket_id IN
    (SELECT ticket_id
    FROM 
        (SELECT *,
         ROW_NUMBER() OVER( PARTITION BY ticket_id
        ORDER BY  ticket_id ) AS row_num
        FROM ticket_data201803) t
        WHERE t.row_num > 1);

-- Create new table "ticket_data_no_dups" and count the records
CREATE TABLE ticket_data_no_dups
AS 
SELECT
  *
FROM 
    ticket_data201803;

SELECT COUNT(*) FROM ticket_data_no_dups;
-- Number of records remaining in the table: 14111

-- Missing ticket_ids
SELECT 
    *
FROM 
    ticket_data_no_dups
WHERE 
    ticket_id IS NULL;
-- Missing created_times
SELECT 
    *
FROM 
    ticket_data_no_dups
WHERE 
    created_time IS NULL;

-- Remove the records with missing ticket_ids and created_times
CREATE TABLE ticket_data_cleaned
AS 
SELECT
  *
FROM ticket_data_no_dups;

SELECT * FROM ticket_data_cleaned;
-- The table ticket_data_cleaned ended up with 14031 records

-- Calculate the total time (in seconds) it took to resolve the support ticket
SELECT 
	ticket_id,
	status,
	type,
	course,
	created_time,
	resolved_time,
  ((EXTRACT(MINUTE FROM(resolved_time - created_time)))*60) AS resolved_time_sec
FROM 
    ticket_data_cleaned
WHERE 
    ticket_id = 2004141 OR ticket_id = 2004632 OR ticket_id = 2004120 OR ticket_id = 2003772
OR ticket_id = 2003713 OR ticket_id = 2003510 OR ticket_id = 2005071 OR ticket_id = 2005148
OR ticket_id =2005142 OR ticket_id = 2004381
ORDER BY 
    created_time DESC;

-- Create a summary by agent
SELECT
	agent_id,
	COUNT(ticket_id) AS num_of_tickets,
	AVG((EXTRACT(MINUTE FROM(resolved_time - created_time)))*60) AS avg_resolved_sec,
	SUM(agent_interactions) AS num_agent_interactions
FROM 
    ticket_data_cleaned
GROUP BY 
    agent_id
ORDER BY 
    num_of_tickets DESC;

-- Create a summary by ticket type
SELECT
	type_,
	COUNT(ticket_id) AS num_of_tickets,
	AVG((EXTRACT(MINUTE FROM(resolved_time - created_time)))*60) AS avg_resolved_sec
FROM 
    ticket_data_cleaned
GROUP BY 
    type_
ORDER BY 
    num_of_tickets DESC;

-- Create a summary by course
SELECT 
	num_of_tickets,
	avg_resolved_sec,
	num_of_students,
	(num_of_tickets::float / num_of_students::float) AS tickets_per_student
FROM
(
	SELECT
	    course,
	    COUNT(ticket_id) AS num_of_tickets,
	    AVG((EXTRACT(MINUTE FROM(resolved_time - created_time)))*60) AS avg_resolved_sec,
	    COUNT(DISTINCT user_id) AS num_of_students
	FROM 
        ticket_data_cleaned
	GROUP BY 
        course
	ORDER BY 
        num_of_tickets DESC
) AS summary;

-- Are there any students who completed multiple courses?
SELECT
	user_id,
	COUNT(DISTINCT course) AS num_of_courses_taken
FROM
	ticket_data_cleaned
GROUP BY 
    user_id
HAVING 
    COUNT(course) > 1
ORDER BY 
    num_of_courses_taken DESC;

-- Which courses did the repeat students do?
SELECT
	user_id,
	course,
	COUNT(ticket_id) AS num_of_tickets,
	RANK() OVER(ORDER BY course ASC) AS order_id
FROM 
	ticket_data_cleaned
GROUP BY 
	user_id, course
ORDER BY 
	user_id ASC;
-- Not sure about the part I have to order the courses completed by the each student as order_id
-- I am still debating between the functions RANK or DENSE_RANK


-- Which courses were the most popular amongst repeat students?
SELECT
	course,
	COUNT(course) AS num_of_courses_taken
FROM 
	ticket_data_cleaned
GROUP BY 
	course
HAVING 
	COUNT(DISTINCT ticket_id) > 1
ORDER BY 
	num_of_courses_taken DESC;

-- Get latest interaction that was made with each student
SELECT
 	t2.user_id,
	t1.ticket_id,
	t1.created_time,
	t1.type_,
	t1.agent_interactions AS num_agent_interactions
FROM 
	ticket_data_cleaned AS t1
INNER JOIN 
	top_15_MIT_AI_2018_01_31 AS t2
ON 
	t1.user_id = t2.user_id
GROUP BY 
	t2.user_id, t1.ticket_id, t1.created_time, t1.type_, num_agent_interactions
ORDER BY
	t2.user_id ASC;
