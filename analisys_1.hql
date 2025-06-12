-- análisis1.sql y análisis1.hql
SELECT 
  make_name,
  model_name,
  COUNT(*) AS total_cars,
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  ROUND(AVG(price), 2) AS avg_price,
  COLLECT_SET(year) AS years
FROM cars
GROUP BY make_name, model_name
ORDER BY make_name;
