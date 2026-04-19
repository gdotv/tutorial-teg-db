MATCH (p:Paper)-[c:CITES]->(adam:Paper)
WHERE adam.`~id` = 'arxiv_1353'
RETURN p, c, adam
LIMIT 10
