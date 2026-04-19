MATCH (u:User)-[p:POSTED]->(s:Subreddit)
RETURN u, p, s
LIMIT 15
