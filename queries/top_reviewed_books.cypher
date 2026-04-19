MATCH (u:User)-[r:REVIEWED]->(b:Book)
WITH b, count(r) AS review_count
ORDER BY review_count DESC
LIMIT 5
RETURN b.`~id` AS id, review_count
