package kafka

import (
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Consumer представляет групп-консьюмера для Sarama.
type GroupConsumer struct {
	// ready канал закрывается, когда консьюмер готов к работе.
	ready chan bool
	// critical канал закрывается, когда консьюмер больше не может
	// корректно продолжать работу.
	critical chan bool
	// mu мьютекс защищает список партии сообщений.
	mu sync.Mutex
	// session содержит текущую сессию.
	session sarama.ConsumerGroupSession
	// messages содержит партию сообщений на обработку.
	messages []*sarama.ConsumerMessage
	// limit определяет максимальный размер партии сообщений.
	limit int
	// cutoff определяет как долго ждать накопление партии сообщений.
	cutoff time.Duration
	// lastProcess содержит время последней обработки сообщений.
	lastProcess time.Time
}

func NewConsumerGroup(
	limit int,
	interval time.Duration,
) *GroupConsumer {
	c := &GroupConsumer{
		ready:       make(chan bool),
		critical:    make(chan bool),
		limit:       limit,
		cutoff:      interval,
		lastProcess: time.Now(),
	}
	return c
}

func (c *GroupConsumer) Setup(session sarama.ConsumerGroupSession) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.session = session
	close(c.ready)

	return nil
}

func (c *GroupConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.processPart()
	c.session = nil
	return nil
}

func (c *GroupConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case <-c.critical:
			return nil
		default:
		}

		select {
		case <-session.Context().Done():
			return nil
		case <-c.critical:
			return nil
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			c.handleMessage(message)
		}
	}
}

func (c *GroupConsumer) Ready() <-chan bool {
	return c.ready
}

func (c *GroupConsumer) Critical() <-chan bool {
	return c.critical
}

func (c *GroupConsumer) makeReadyAgain() {
	c.ready = make(chan bool)
}

// check выполняет проверку состояния и выполняет нужные действия.
func (c *GroupConsumer) check() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.lastProcess) >= c.cutoff {
		c.processPart()
	}
}

// handleMessage обрабатывает новое прочитанное сообщение.
func (c *GroupConsumer) handleMessage(message *sarama.ConsumerMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, message)
	if len(c.messages) >= c.limit {
		c.processPart()
	}
}

func (c *GroupConsumer) processPart() {
	if len(c.messages) == 0 {
		return
	}
	for _, message := range c.messages {
		log.Println("read:", string(message.Value))
	}

	//c.log.DebugF("обработана пачка записей: удалено %d, обновлено %d", deleteCount, updateCount)

	// Коммит в Кафку и обнуление состояния
	for _, message := range c.messages {
		c.session.MarkMessage(message, "read done")
	}
	c.session.Commit()
	c.messages = nil
	c.lastProcess = time.Now()
}
func (c *GroupConsumer) MakeReadyAgain() {
	c.ready = make(chan bool)
}
