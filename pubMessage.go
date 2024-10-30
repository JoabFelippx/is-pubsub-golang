package main

import (

        "log"

        "rabbit_ingo/is/utils/common"

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
    topic := "Position.golang"

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

        pos := &is_common.Position{}

        pos.X = 1
        pos.Y = 2

        data, err := proto.Marshal(pos)

        log.Printf("Sending message: %v", pos)
        if err != nil {
                log.Fatalf("Failed to marshal a message: %v", err)
        }

        err = channel.Publish(
                exchange,
                topic,
                false,
                false,
                amqp.Publishing{
                        ContentType: "application/x-protobuf",
                        Body:        data,
                },
        )

        if err != nil {
                log.Fatalf("Failed to publish a message: %v", err)
        }

}



