MATCH (a:Paper)<-[e:CITES]-(b:Paper)
WITH a, e, b LIMIT 3
RETURN a AS source, e AS edge, b AS target
UNION ALL
MATCH (a:Reviewer)-[e:REVIEWED]->(b:Product)
WITH a, e, b LIMIT 3
RETURN a AS source, e AS edge, b AS target
UNION ALL
MATCH (a:User)-[e:POSTED]->(b:Subreddit)
WITH a, e, b LIMIT 3
RETURN a AS source, e AS edge, b AS target
UNION ALL
MATCH (a:User)-[e:REVIEWED]->(b:Book)
WITH a, e, b LIMIT 3
RETURN a AS source, e AS edge, b AS target

