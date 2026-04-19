MATCH (u:User)-[r:REVIEWED]->(b:Book)
WHERE b.`~id` = 'goodreads_children_86'
AND (r.score <= 1.0 OR r.score >= 5.0)
RETURN u, r, b
LIMIT 12
