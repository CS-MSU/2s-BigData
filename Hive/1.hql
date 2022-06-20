CREATE TABLE 
  mirpulatov.questions AS
SELECT
  id, 
  tags
FROM
  default.posts
WHERE 
  tags like "%hadoop%" AND posttypeid = 1;
