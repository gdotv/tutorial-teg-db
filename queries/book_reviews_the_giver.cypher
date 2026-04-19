MATCH (u:User)-[r:REVIEWED]->(b:Book)
WHERE b.`~id` = 'goodreads_children_71'
RETURN u, r, b
LIMIT 10
