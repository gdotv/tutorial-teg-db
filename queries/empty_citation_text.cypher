MATCH (a:Paper)-[e:CITES]->(b:Paper)
WHERE e.text IS NULL
RETURN a, e, b
LIMIT 5
