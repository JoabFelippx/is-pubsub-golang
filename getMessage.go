package main

import (
	"log"

	"rabbit_ingo/is/utils/vision"

	"google.golang.org/protobuf/proto"

	amqp "github.com/rabbitmq/amqp091-go"
)

func connect(broker_uri string) *amqp.Connection {
	conn, err := amqp.Dial(broker_uri)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	return conn
}

func createChannel(conn *amqp.Connection) *amqp.Channel {

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	return ch
}

func queueDeclare(ch *amqp.Channel) amqp.Queue {
	queue, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	return queue
}

func main() {

	broker_uri := "amqp://guest:guest@10.10.2.211:30000/"
	exchange := "is"
    topic := "Robis.1.Detection"

	conn := connect(broker_uri)
	defer conn.Close()

	log.Print("Connected to RabbitMQ")

    channel := createChannel(conn)
	defer channel.Close()
	

	channel.ExchangeDeclare(
		exchange,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)

	queue := queueDeclare(channel)

	channel.QueueBind(
		queue.Name,
		topic,
		exchange,
		false,
		nil,
	)

	msgs, err := channel.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {	
		for d := range msgs {

			objs := &is_vision.ObjectAnnotations{}

			err := proto.Unmarshal(d.Body, objs)

			if err != nil {
				log.Fatalf("Failed to unmarshal the message: %v", err)
			}

			log.Printf("Received a message: %v", objs)
		}
	} ()

	<-forever

}

