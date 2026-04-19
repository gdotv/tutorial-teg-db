MATCH (a:Paper)-[e:CITES]->(b:Paper)
WHERE (a.`~id` = 'arxiv_67166' AND b.`~id` = 'arxiv_25208')
   OR (a.`~id` = 'arxiv_25208' AND b.`~id` = 'arxiv_67166')
RETURN a, e, b
