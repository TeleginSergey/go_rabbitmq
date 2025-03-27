package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"pub_sub_rabbit/internal/rabbitmq"
	"time"
)

var db *sql.DB

func main() {
	initDB()
	defer db.Close()

	role := os.Getenv("CONTAINER_ROLE")

	switch role {
	case "publisher":
		setupRabbitMQ()
		runPublisher()
		os.Exit(0)
	case "consumer1", "consumer2":
		runConsumer(role)
	default:
		log.Fatal("Unknown CONTAINER_ROLE. Use 'publisher', 'consumer1' or 'consumer2'")
	}
}

func initDB() {
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable",
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"),
		os.Getenv("DB_USER"), os.Getenv("DB_NAME"), os.Getenv("DB_PASSWORD"))

	var err error
	for i := 0; i < 5; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil {
			err = db.Ping()
			if err == nil {
				break
			}
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil {
		log.Fatalf("Ошибка подключения к БД: %v", err)
	}

	for _, consumer := range []string{"consumer1", "consumer2"} {
		_, err = db.Exec(fmt.Sprintf(`
            CREATE TABLE messages_%s (
				message_id VARCHAR(64) PRIMARY KEY,
				content TEXT NOT NULL,
				processed_at TIMESTAMP DEFAULT NOW()
			);`, consumer))
		if err != nil {
			log.Printf("Предупреждение: не удалось создать таблицу messages_%s: %v", consumer, err)
		}
	}
}

func setupRabbitMQ() {
	conn, err := rabbitmq.ConnectWithRetry("amqp://guest:guest@rabbitmq:5672/", 10, 3*time.Second)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"messages",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	log.Println("RabbitMQ setup completed successfully")
}

func runPublisher() {
	time.Sleep(5 * time.Second)

	pub, err := rabbitmq.NewPublisher("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		log.Fatalf("[publisher] Failed to create publisher: %v", err)
	}
	defer pub.Shutdown()

	if err := pub.PublishFromFile("files/input.txt"); err != nil {
		log.Fatalf("[publisher] Failed to publish messages: %v", err)
	}

	log.Println("[publisher] All messages published successfully")
}

func runConsumer(consumerName string) {
	if db == nil {
		log.Fatalf("[%s] Database not initialized", consumerName)
	}

	const maxRetries = 5
	const retryDelay = 3 * time.Second

	for retry := 0; retry < maxRetries; retry++ {
		consumer, err := rabbitmq.NewConsumer("amqp://guest:guest@rabbitmq:5672/", db)
		if err != nil {
			log.Printf("[%s] Connection error (attempt %d/%d): %v",
				consumerName, retry+1, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		queueName := fmt.Sprintf("queue_%s", consumerName)
		if err := consumer.StartConsuming("messages", queueName, consumerName); err != nil {
			log.Printf("[%s] Consume error (attempt %d/%d): %v",
				consumerName, retry+1, maxRetries, err)
			consumer.Shutdown()
			time.Sleep(retryDelay)
			continue
		}

		log.Printf("[%s] Successfully started", consumerName)
		<-consumer.Done
		consumer.Shutdown()
		return
	}

	log.Fatalf("[%s] Failed after %d attempts", consumerName, maxRetries)
}
