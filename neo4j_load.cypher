DROP CONSTRAINT UniqueIndividuals;
DROP CONSTRAINT UniqueCorps;
MATCH (n) DETACH DELETE n;

CREATE CONSTRAINT UniqueIndividuals FOR (i:Individuals) REQUIRE i.id_ind IS UNIQUE;

:auto
LOAD CSV  WITH HEADERS FROM 'file:///inds.csv' AS row
FIELDTERMINATOR '|'
CALL {
  WITH row
  MERGE(i:Individuals {id_ind: row.id})
    SET i.fullname = row.fullname_simp, i.address = row.address_simp, i.group_network = row.group_network
} IN TRANSACTIONS OF 5000 ROWS;

CREATE CONSTRAINT UniqueCorps FOR (c:Corps) REQUIRE c.id_corp IS UNIQUE;

:auto
LOAD CSV  WITH HEADERS FROM 'file:///corps.csv' AS row
FIELDTERMINATOR '|'
CALL {
  WITH row
  MERGE(c:Corps {id_corp: row.id})
    SET c.entityname = row.entityname, c.group_network = row.group_network
} IN TRANSACTIONS OF 5000 ROWS;

:auto
LOAD CSV  WITH HEADERS FROM 'file:///edges.csv' AS row
FIELDTERMINATOR '|'
CALL {
  WITH row
  MATCH (c1:Corps {id_corp: row.id_corp})
  MATCH (c2:Corps {id_corp: row.id_link})
  MERGE (c1)-[:RELATED]-(c2)
} IN TRANSACTIONS OF 5000 ROWS;

:auto
LOAD CSV  WITH HEADERS FROM 'file:///edges.csv' AS row
FIELDTERMINATOR '|'
CALL {
  WITH row
  MATCH (c1:Corps {id_corp: row.id_corp})
  MATCH (c2:Corps {id_corp: row.id_link})
  MATCH (i:Individuals {id_ind: row.id_link})
  MERGE (c1)-[:RELATED]-(c2)
  MERGE (i)-[:AGENT_OF]->(c1)
} IN TRANSACTIONS OF 5000 ROWS;