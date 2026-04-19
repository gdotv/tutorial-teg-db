MATCH (u:User)-[r:REVIEWED]->(b:Book)
WHERE r.score <= 1.0
WITH b, count(r) AS negative
WHERE negative > 10
MATCH (u2:User)-[r2:REVIEWED]->(b)
WHERE r2.score >= 5.0
WITH b, negative, count(r2) AS positive
WHERE positive > 10
RETURN b.`~id` AS book_id, negative, positive
ORDER BY negative DESC
LIMIT 5
