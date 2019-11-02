//create a business unique id for a friend referral system
CREATE CONSTRAINT ON (p:Person) ASSERT p.id IS UNIQUE;

//create nodes
CREATE (p: Person {id: "di@zetemhas.vg"}) SET p.firstname="Jack", p.lastname="Singleton", p.city="Johannesburg", p.age=36;
CREATE (p: Person {id: "uwa@tod.ky"}) SET p.firstname="Patrick", p.lastname="McGee", p.city="Auckland", p.age=62;
CREATE (p: Person {id: "so@jejap.gg"}) SET p.firstname="Roger", p.lastname="Palmer", p.city="Tokyo", p.age=26;
CREATE (p: Person {id: "ibe@ari.tc"}) SET p.firstname="Mildred", p.lastname="Joshua", p.city="New York", p.age=63;
CREATE (p: Person {id: "sac@nirwa.ge"}) SET p.firstname="Glen", p.lastname="Warren", p.city="St. Petersburg", p.age=56;
CREATE (p: Person {id: "unki@guwul.pt"}) SET p.firstname="Eric", p.lastname="Owen", p.city="Amsterdam", p.age=18;
CREATE (p: Person {id: "mekwa@jeh.mz"}) SET p.firstname="Callie", p.lastname="Graves", p.city="Lagos", p.age=60;
CREATE (p: Person {id: "gohaeta@unowoet.ee"}) SET p.firstname="Shane", p.lastname="Wise", p.city="Berlin", p.age=34;
CREATE (p: Person {id: "zo@umahulog.am"}) SET p.firstname="Emily", p.lastname="Johnston", p.city="London", p.age=65;
CREATE (p: Person {id: "lo@ro.ga"}) SET p.firstname="Eleanor", p.lastname="Mack", p.city="Los Angeles", p.age=28;

//create relationships
MATCH (jack: Person {id: "di@zetemhas.vg"})
MATCH (pat: Person {id: "uwa@tod.ky"})
MATCH (rog: Person {id: "so@jejap.gg"})
MATCH (josh: Person {id: "ibe@ari.tc"})
MATCH (glen: Person {id: "sac@nirwa.ge"})
MATCH (eric: Person {id: "unki@guwul.pt"})
MATCH (cal: Person {id: "mekwa@jeh.mz"})
MATCH (shane: Person {id: "gohaeta@unowoet.ee"})
MATCH (emy: Person {id: "zo@umahulog.am"})
MATCH (mac: Person {id: "lo@ro.ga"})
MERGE (jack)-[:REFERS]->(pat)
MERGE (jack)-[:REFERS]->(rog)
MERGE (pat)-[:REFERS]->(cal)
MERGE (cal)-[:REFERS]->(eric)
MERGE (cal)-[:REFERS]->(jack)
MERGE (cal)-[:REFERS]->(rog)
MERGE (rog)-[:REFERS]->(jack)
MERGE (rog)-[:REFERS]->(pat)
MERGE (glen)-[:REFERS]->(eric)
MERGE (mac)-[:REFERS]->(eric)
MERGE (cal)-[:REFERS]->(glen)
MERGE (shane)-[:REFERS]->(mac)
MERGE (josh)-[:REFERS]->(josh);

//write queries

//find all
MATCH (p:Person) return p;

//find those who referred themselves
MATCH (p:Person)-[:REFERS]-> (p1)
WHERE p.id = p1.id
return p.firstname, p.lastname, p.id AS email, p.age;

//find all that referred eric
MATCH (p:Person {id: "unki@guwul.pt"}) <- [:REFERS] - (p1) return p1; 

//find all that eric referred
MATCH (p:Person {id: "unki@guwul.pt"}) - [:REFERS] -> (p1) return p1; 

//find all that Patrick referred or was referred by him
MATCH (p:Person {id: "uwa@tod.ky"}) - [:REFERS] - (p1) return p1; 

//generate a password for everyone in our network
MATCH (p:Person)
SET p.passwd = apoc.text.random(16, "A-Za-z0-9")
return p.passwd;

//remove the password property 
MATCH (p:Person)
REMOVE p.passwd
return p;

MATCH (p:Person)-[r:REFERS]->()
DELETE r
DELETE p;

//another way to do it
MATCH p=()-->() DELETE p;

//or
MATCH p=() DELETE p;