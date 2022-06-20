CREATE TABLE 
  mirpulatov.result AS
SELECT
  answers.user_id id, 
  SUM(answers.score) sum_score, 
  ru_users.name name, 
  ru_users.location location
FROM
  answers
INNER JOIN
  ru_users
ON
  ru_users.id = answers.user_id
GROUP BY
  answers.user_id, 
  ru_users.name, 
  ru_users.location
ORDER BY
  sum_score DESC;
