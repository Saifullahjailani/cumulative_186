-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0
CREATE VIEW q0(era)
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear 
  FROM people
  WHERE  weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people 
  WHERE namefirst LIKE '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height) AS avgheight, COUNT(*) AS count
  FROM people 
  GROUP BY birthyear 
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, AVG(height) as avgheight, COUNT(*) as count 
  FROM people
  GROUP BY birthyear
  HAVING AVG(height) > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, p.playerID AS playerid, yearid
  FROM people AS p, halloffame AS f
  WHERE p.playerID = f.playerID 
  AND LOWER(f.inducted) = 'y'
  ORDER BY f.yearid DESC , f.playerID ASC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, p.playerid, s.schoolid, h.yearid
  FROM people p, halloffame h, schools s, CollegePlaying col
  WHERE p.playerID = col.playerID
  AND p.playerID = h.playerID 
  AND col.schoolID = s.schoolID
  AND LOWER(s.schoolState) = 'ca'
  AND LOWER(inducted) = 'y'
  ORDER BY h.yearid DESC, s.schoolID, p.playerid ASC
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT p.playerID, p.namefirst, p.namelast, col.schoolID
  FROM people AS p INNER JOIN halloffame AS h
  ON p.playerID = h.playerID
  LEFT OUTER JOIN CollegePlaying AS col
  ON h.playerID = col.playerid
  where LOWER(inducted) = 'y'
  ORDER BY p.playerID DESC, col.schoolID ASC
;



-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, b.yearid, 1.0 * (b.H + b.H2B + 2*b.H3B + 3*b.HR)/b.AB as slg
  FROM people p, batting b
  WHERE p.playerid = b.playerID AND b.AB > 50 
  ORDER BY slg DESC, b.yearid, p.playerID ASC
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT p.playerid, p.namefirst, p.namelast, 1.0 * (SUM(b.H) + SUM(b.H2B) + SUM(2*b.H3B) + SUM(3*b.HR))/SUM(b.AB) as lslg
  FROM people p INNER JOIN batting b
  ON p.playerID = b.playerID
  GROUP BY p.playerid
  HAVING SUM(b.AB) > 50
  ORDER BY lslg DESC, p.playerID ASC
  LIMIT 10
  
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT p.namefirst, p.namelast, 1.0 * (SUM(b.H) + SUM(b.H2B) + SUM(2*b.H3B) + SUM(3*b.HR))/SUM(b.AB) as lslg
  FROM people p INNER JOIN batting b
  ON p.playerID = b.playerID
  GROUP BY p.playerid
  HAVING SUM(b.AB) > 50
  AND 1.0 * (SUM(b.H) + SUM(b.H2B) + SUM(2*b.H3B) + SUM(3*b.HR))/SUM(b.AB) > 
  (SELECT 1.0 * (SUM(b.H) + SUM(b.H2B) + SUM(2*b.H3B) + SUM(3*b.HR))/SUM(b.AB) as mayslslg
  FROM people p INNER JOIN batting b
  ON p.playerID = b.playerID
  WHERE p.playerID = 'mayswi01'
  GROUP BY p.playerid
  )
  ORDER BY lslg DESC, p.playerID ASC
  
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg)
AS
  SELECT yearID, MIN(salary), MAX(salary), AVG(salary)
  FROM people AS p INNER JOIN salaries AS s
  ON p.playerID = s.playerID
  GROUP BY yearID
  ORDER BY yearID ASC
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH table2016 AS (
    SELECT *
    FROM salaries
    WHERE yearID = 2016
  ), cnt AS (
    SELECT MAX(salary) AS h, MIN(salary) AS l
    FROM table2016 INNER JOIN people
    ON people.playerID = table2016.playerID
  ), dist AS (
    SELECT (h-l)/10 AS diff
    FROM cnt
  ),lowrange AS (
    SELECT ROW_NUMBER() OVER() -1 AS b, r.value * 1.0 AS range
    FROM cnt, GENERATE_SERIES(l, h, dist.diff) AS r, dist
  ), highrange AS (
    SELECT ROW_NUMBER() OVER() -1 AS b, lowrange.range + diff AS range
    FROM lowrange, dist
  )
  SELECT lowrange.b AS binid, lowrange.range AS lowRange, highrange.range AS highRange, COUNT(*) AS count
  FROM table2016, lowrange INNER JOIN highrange
  ON lowrange.b = highrange.b
  WHERE (lowrange.b < 9 AND salary >= lowrange.range AND salary < highrange.range) OR (lowrange.b = 9 AND salary >= lowrange.range AND salary <= highrange.range)
  GROUP BY lowrange.b
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH currentYear AS (
    SELECT yearID, MIN(salary) AS mn, MAX(salary) AS mx, AVG(salary) AS avg
    FROM salaries
    GROUP BY yearID 
  ), 
  nextYear AS (
     SELECT yearID + 1 AS nID, mn, mx, avg 
     FROM currentYear
  )
  SELECT nID, currentYear.mn - nextYear.mn, currentYear.mx - nextYear.mx, currentYear.avg - nextYear.avg 
  FROM nextYear, currentYear
  WHERE nID = yearID
  ORDER BY nID ASC

;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  WITH maxSalaries AS (
    SELECT yearID, MAX(salary) AS mx
    FROM salaries
    GROUP BY yearID
  ), playersMax AS (
    SELECT mx.yearID, playerID, mx.mx AS salary
    FROM salaries AS s, maxSalaries AS mx
    WHERE s.yearID = mx.yearID AND s.salary = mx.mx AND (s.yearID = 2000 OR s.yearID = 2001)
  )
  SELECT p.playerID, p.namefirst, p.namelast, mx.salary, mx.yearID
  FROM playersMax AS mx, people AS p
  where p.playerID = mx.playerID
  ORDER BY mx.yearID ASC
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT a.teamID, MAX(salary) - MIN(salary)
  FROM allstarfull AS a INNER JOIN salaries AS S
  ON a.playerID = s.playerID AND a.yearID = s.yearID
  WHERE s.yearID = 2016
  GROUP BY a.teamID

;

