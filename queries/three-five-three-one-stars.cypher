MATCH (u:User)-[r:REVIEWED]->(b:Book)
WHERE b.`~id` = '<BOOK_ID>' AND r.score <= 1.0
WITH u, r, b LIMIT 3
RETURN u AS user, r AS review, b AS book
UNION ALL
MATCH (u:User)-[r:REVIEWED]->(b:Book)
WHERE b.`~id` = '<BOOK_ID>' AND r.score >= 5.0
WITH u, r, b LIMIT 3
RETURN u AS user, r AS review, b AS book

