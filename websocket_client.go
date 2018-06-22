package middlewares

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type writingMessage struct {
	messageType int
	data        []byte
}

/*
WebsocketClient - client that listens for events and sends actions to Websocket server
*/
type WebsocketClient struct {
	Host
	Conn         *websocket.Conn
	dataHandler  func(interface{})
	errorHandler func(interface{})
	writeChan    chan writingMessage
	writeErrChan chan error
	initData     sync.Once
}

/*
OnData - handler will handle incoming data
*/
func (ws *WebsocketClient) OnData(h func(interface{})) {
	ws.dataHandler = h
}

/*
OnError - handler will handle incoming data
*/
func (ws *WebsocketClient) OnError(h func(interface{})) {
	ws.errorHandler = h
}

/*
Connect - connecting to WS server
*/
func (ws *WebsocketClient) Connect() error {
	flag.Parse()
	log.SetFlags(0)

	url := ws.getAddress()
	log.Printf("[INFO] Connecting to %s", url)

	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	ws.Conn = c
	if err != nil {
		return err
	}
	ws.initData.Do(func() {
		ws.writeChan = make(chan writingMessage)
		ws.writeErrChan = make(chan error)
		go func() {
			for {
				message := <-ws.writeChan
				ws.writeErrChan <- ws.Conn.WriteMessage(message.messageType, message.data)
			}
		}()
	})
	log.Println("[WS] Connected to server")
	return nil
}

/*
Listen — starting to listen to WS server
*/
func (ws *WebsocketClient) Listen() {
	if ws.Conn == nil {
		ws.Connect()
	}
	checkConnDone := make(chan struct{})
	ws.Conn.SetPongHandler(func(string) error {
		ws.Conn.SetReadDeadline(time.Time{})
		return nil
	})
	go ws.checkConnection(checkConnDone)
	go func(checkConnDone chan<- struct{}) {
		defer close(checkConnDone)
		defer ws.Conn.Close()
		for {
			var message interface{}
			// t, m, err := ws.Conn.ReadMessage()
			// fmt.Println(t, m, err)
			err := ws.Conn.ReadJSON(&message)

			if err != nil {
				go ws.handleError(err)
				return
			}
			go ws.handleData(message)
		}
	}(checkConnDone)
}

func (ws *WebsocketClient) checkConnection(done <-chan struct{}) {
	var timer *time.Timer
	pingInterval := 10 * time.Second
	pongWait := 9 * time.Second // less than pingInterval
	for {
		ws.Conn.SetReadDeadline(time.Now().Add(pongWait))
		if err := ws.writeMessage(websocket.PingMessage, []byte("PING")); err != nil {
			ws.Conn.SetReadDeadline(time.Time{})
			log.Println("[ERROR][WS] Sending ping:", err)
			return
		}
		if timer == nil {
			timer = time.NewTimer(pingInterval)
		}
		select {
		case <-done:
			return
		case <-timer.C:
			timer.Reset(pingInterval)
		}
	}
}

func (ws *WebsocketClient) writeMessage(messageType int, data []byte) error {
	ws.writeChan <- writingMessage{messageType: messageType, data: data}
	err := <-ws.writeErrChan
	return err
}

func (ws *WebsocketClient) handleData(data interface{}) {
	if ws.dataHandler != nil {
		ws.dataHandler(data)
	}
}
func (ws *WebsocketClient) handleError(err interface{}) {
	log.Println("[ERROR][WS] Read error:", err)
	if ws.errorHandler != nil {
		ws.errorHandler(err)
	}
}

/*
Send - sending action to WS server. May return error
*/
func (ws *WebsocketClient) Send(data interface{}) error {
	req, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return ws.writeMessage(websocket.TextMessage, req)
}

func (ws *WebsocketClient) getAddress() string {
	return "ws://" + ws.Host.Host + ":" + strconv.Itoa(ws.Host.Port) + ws.Host.Path
}

/*
Close - closing connection
*/
func (ws *WebsocketClient) Close() error {
	fmt.Println("[WS][CLIENT] Closing connection")
	if ws.Conn != nil {
		return ws.writeMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		)
	}
	return nil
}
