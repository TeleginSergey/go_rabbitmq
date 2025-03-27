## Интеграция Redis в систему обработки сообщений

### 1. Хранение данных в Redis
- **Формат ключей**:
```sh
  message:{sha256_hash}:status  # Пример: some_message:e4d909c290...:processed
```
- **Структура данных hash (пример)**
```json
{
  "status": "processing|processed",
  "consumer": "consumer1",
  "timestamp": "2023-05-21T12:00:00Z"
}
```
### 2. Защита от дублирования
- **Механизм блокировок:**
  // Код из handleDeliveries():
  if ok, _ := c.redisClient.HSetNX(ctx, key, "status", "processing").Result(); !ok {
  // Сообщение уже обрабатывается
  }
- **Двухуровневая проверка:**
    1. Проверка Redis (статус processing/processed)
    2. Резервная проверка в PostgreSQL

### 3. Управление памятью
- **TTL:**
  - Блокировки: 5 минут
  - Результаты: 24 часа

### 4. Обработка сбоев
- **При падении Redis:**
  - Автоматический переход на проверку БД
- **Гарантии**
  - Exactly-once обработка

### 5. Решение проблемы гонки
- **Атомарные операции**
```
// В handleDeliveries():
c.redisClient.HSetNX(ctx, key, "status", "processing")
c.redisClient.Expire(ctx, key, 24*time.Hour)
```
- **Контрольные точки:**
  - Уникальные ID сообщений
  - Таймауты блокировок
  - Транзакционность ON CONFLICT в БД
