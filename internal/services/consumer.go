package services

import (
	"log"

	"github.com/gocql/gocql"
)

func initCassandra() *gocql.Session {
	cluster := gocql.NewCluster("cassandra")
	cluster.Keyspace = "mykeyspace"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %v", err)
	}
	return session
}

func SaveMessageToCassandra(message string, table string) {
	session := initCassandra()
	defer session.Close()

	query := "INSERT INTO " + table + " (id, message) VALUES (uuid(), ?)"
	if err := session.Query(query, message).Exec(); err != nil {
		log.Printf("Failed to insert into Cassandra: %v", err)
	}
}