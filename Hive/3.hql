CREATE TABLE 
  mirpulatov.ru_users AS
SELECT
  id, 
  displayname name, 
  location
FROM
  default.users
WHERE
  LOWER(location) LIKE '%russia%' OR
  LOWER(location) LIKE '%moscow%' OR
  LOWER(location) LIKE '%novosibirsk%' OR
  LOWER(location) LIKE '%saint_petersburg%' OR
  LOWER(location) LIKE '%yekaterinburg%' OR
  LOWER(location) LIKE '%kazan%' OR
  LOWER(location) LIKE '%nizhny_novgorod%' OR
  LOWER(location) LIKE '%chelyabinsk%' OR
  LOWER(location) LIKE '%samara%' OR
  LOWER(location) LIKE '%omsk%' OR
  LOWER(location) LIKE '%rostov_on_don%' OR
  LOWER(location) LIKE '%ufa%' OR
  LOWER(location) LIKE '%krasnoyarsk%' OR
  LOWER(location) LIKE '%voronezh%' OR
  LOWER(location) LIKE '%perm%' OR
  LOWER(location) LIKE '%volgograd%';
