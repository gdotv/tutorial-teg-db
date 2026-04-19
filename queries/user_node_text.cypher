MATCH (u:User)-[:REVIEWED]->(:Book)
RETURN DISTINCT u.`~id` AS id, u.text AS node_text
LIMIT 5
