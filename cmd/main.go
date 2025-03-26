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
		// После завершения работы просто выходим
		os.Exit(0)
	case "consumer1", "consumer2":
		consumerName := os.Getenv("CONSUMER_NAME")
		runConsumer(consumerName)
	default:
		log.Fatal("Unknown CONTAINER_ROLE. Use 'publisher', 'consumer1' or 'consumer2'")
	}
}

func initDB() {
	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable",
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"),
		os.Getenv("DB_USER"), os.Getenv("DB_NAME"), os.Getenv("DB_PASSWORD"))

	var err error
	// Добавляем retry логику
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

	// Улучшенное создание таблиц
	for _, consumer := range []string{"consumer1", "consumer2"} {
		_, err = db.Exec(fmt.Sprintf(`
            CREATE TABLE IF NOT EXISTS messages_%s (
                id BIGSERIAL PRIMARY KEY,
                content TEXT NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT NOW()
            )`, consumer))
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

	// Declare exchange
	err = ch.ExchangeDeclare(
		"messages",
		"fanout",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	log.Println("RabbitMQ setup completed successfully")
}

func runPublisher() {
	time.Sleep(5 * time.Second) // Даем время для инициализации

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
		log.Fatalf("[%s] База данных не инициализирована", consumerName)
	}

	// Конфигурация переподключений
	const maxRetries = 5
	const retryDelay = 3 * time.Second

	for retry := 0; retry < maxRetries; retry++ {
		consumer, err := rabbitmq.NewConsumer("amqp://guest:guest@rabbitmq:5672/")
		if err != nil {
			log.Printf("[%s] Ошибка подключения (попытка %d/%d): %v",
				consumerName, retry+1, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		handler := func(msg []byte) error {
			// Проверяем дубликаты
			var exists bool
			err := db.QueryRow(
				fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM messages_%s WHERE content = $1)", consumerName),
				string(msg),
			).Scan(&exists)

			if err != nil {
				return fmt.Errorf("ошибка проверки сообщения: %v", err)
			}

			if exists {
				log.Printf("[%s] Дубликат сообщения: %s", consumerName, string(msg))
				return nil
			}

			// Сохраняем сообщение
			_, err = db.Exec(
				fmt.Sprintf("INSERT INTO messages_%s (content) VALUES ($1)", consumerName),
				string(msg),
			)
			if err != nil {
				return fmt.Errorf("ошибка сохранения: %v", err)
			}

			log.Printf("[%s] Сообщение обработано: %s", consumerName, string(msg))
			return nil
		}

		queueName := fmt.Sprintf("queue_%s", consumerName)
		err = consumer.StartConsuming("messages", queueName, consumerName, handler)
		if err != nil {
			log.Printf("[%s] Ошибка потребления (попытка %d/%d): %v",
				consumerName, retry+1, maxRetries, err)
			consumer.Shutdown()
			time.Sleep(retryDelay)
			continue
		}

		log.Printf("[%s] Успешно запущен", consumerName)
		<-consumer.Done // Ожидаем завершения
		consumer.Shutdown()
		return
	}

	log.Fatalf("[%s] Не удалось запустить после %d попыток", consumerName, maxRetries)
}
