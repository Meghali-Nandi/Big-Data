mysql -u root -ppassword --local-infile=1 covid


SELECT s.State, r.Gender, COUNT(*)*100.0 / SUM(COUNT(*)) OVER(PARTITION BY r.`State code`) AS percent
FROM state_wise s
JOIN (
  SELECT Gender, `State code` FROM raw_data1
  UNION ALL SELECT Gender, `State code` FROM raw_data2
  UNION ALL SELECT Gender, `State code` FROM raw_data3
  UNION ALL SELECT Gender, `State code` FROM raw_data4
  UNION ALL SELECT Gender, `State code` FROM raw_data5
  UNION ALL SELECT Gender, `State code` FROM raw_data6
  UNION ALL SELECT Gender, `State code` FROM raw_data7
  UNION ALL SELECT Gender, `State code` FROM raw_data8
  UNION ALL SELECT Gender, `State code` FROM raw_data9
  UNION ALL SELECT Gender, `State code` FROM raw_data10
) r ON s.State_code = r.`State code`
GROUP BY s.State, r.Gender, r.`State code`;

///


CREATE INDEX age_idx ON table_name (age);
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
SELECT CONCAT(FLOOR(`Age Bracket`/5)*5, '-', FLOOR(`Age Bracket`/5)*5+4) AS age_group, COUNT(*) AS num_patients
FROM raw_data1
GROUP BY FLOOR(`Age Bracket`/5)
ORDER BY num_patients DESC limit 1;



///
SELECT State,
       SUM(CASE WHEN Patient_Status = 'Recovered' THEN 1 ELSE 0 END) / COUNT(1) AS recovery_rate,
       SUM(CASE WHEN Patient_Status = 'Deceased' THEN 1 ELSE 0 END) / COUNT(1) AS death_rate
FROM death_and_recovered1
GROUP BY State
ORDER BY recovery_rate DESC, death_rate ASC limit 1;

///




