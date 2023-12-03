package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// run this
// docker run --network host -p 4222:4222 nats -js

type payload struct {
	UserID, Msg string
}

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	users map[string]chan string
)

func main() {
	users = make(map[string]chan string)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	cc, err := consume(ctx, js, users)
	if err != nil {
		log.Fatal(err)
	}
	defer cc.Stop()

	r := gin.Default()

	r.GET("/send", func(c *gin.Context) {
		userID := c.Query("user_id")
		if userID == "" {
			c.String(http.StatusBadRequest, "empty user id")
			return
		}

		_, ok := users[userID]
		if !ok {
			c.String(http.StatusNotFound, "cannot find user id")
			return
		}

		msg := c.Query("msg")
		if userID == "" {
			c.String(http.StatusBadRequest, "empty msg")
			return
		}

		// insert the message into the val

		req := payload{
			UserID: userID,
			Msg:    msg,
		}
		reqRaw, _ := json.Marshal(req)
		if _, err := js.Publish(ctx, "FOO.TEST1", []byte(reqRaw)); err != nil {
			fmt.Println("pub error: ", err)
		}
		c.String(http.StatusOK, "Success")
	})

	r.GET("/ws", func(c *gin.Context) {
		w := c.Writer
		r := c.Request
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			fmt.Printf("Failed to set websocket upgrade: %+v\n", err)
			return
		}

		userID := r.FormValue("user_id")
		fmt.Println(userID)
		users[userID] = make(chan string, 100)
		conn.WriteMessage(websocket.TextMessage, []byte("user registered"))

		for msg := range users[userID] {
			conn.WriteMessage(websocket.TextMessage, []byte(msg))
		}
	})

	r.Run(":8080")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

}

func consume(ctx context.Context, js jetstream.JetStream, users map[string]chan string) (jetstream.ConsumeContext, error) {
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST_STREAM",
		Subjects: []string{"FOO.*"},
	})
	if err != nil {
		return nil, err
	}

	cons, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "TestConsumerConsume",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, err
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		fmt.Println("got data", string(msg.Data()))
		res := payload{}
		err := json.Unmarshal(msg.Data(), &res)
		if err != nil {
			msg.Nak()
			return
		}
		chanUser, ok := users[res.UserID]
		if !ok {
			msg.Nak()
			return
		}

		// assign chan user
		chanUser <- res.Msg
		msg.Ack()
	}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
		fmt.Println(err)
	}))
	if err != nil {
		return nil, err
	}

	return cc, nil
}
