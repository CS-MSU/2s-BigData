CREATE TABLE 
  mirpulatov.answers AS
SELECT
  owneruserid user_id, 
  questions.tags tags, 
  score
FROM 
  default.posts
INNER JOIN
  questions
ON 
  questions.id = parentid;
