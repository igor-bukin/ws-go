package demo

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/google/uuid"
	"gitlab.yalantis.com/api/src/logger"
	"gitlab.yalantis.com/api/src/models"
	"gitlab.yalantis.com/api/src/persistence/postgres"
	redisCli "gitlab.yalantis.com/api/src/persistence/redis"
	"gitlab.yalantis.com/api/src/services/jwt"
	httperrors "gitlab.yalantis.com/data-models/http-errors"
	"go.uber.org/zap"
)

type service struct {
	rdb redisCli.RedisCli
	pg  *postgres.Client
	jwt jwt.Service
}

const (
	appointmentList = "list:appointment:%s"
	channel         = "appointment:%s"
	updateChannel   = "calendar:%s:update"
	handlerChanSize = 128
	pingTimeout     = 59
)

type WsUser struct {
	ID   string
	io   *sync.Mutex
	conn io.ReadWriteCloser

	channelsHandler map[string]*redis.PubSub
	wsReader        *wsutil.Reader
	practices       []string

	userRole models.UserRoleType
	userID   uuid.UUID
	s        *service

	pingPong    PingPong
	messageChan MessageChan
}

type PingPong struct {
	lastPingSent     time.Time
	pingPongTicker   *time.Ticker
	lastPongReceived time.Time
}

type MessageChan struct {
	messageHandlerChan        chan models.Response
	messageFrameChan          chan RawData
	messageNewAppointmentChan <-chan *redis.Message
	messageUpdateChan         <-chan *redis.Message
	stopListenerChan          chan struct{}
}

type RawData struct {
	msg    []byte
	opCode ws.OpCode
}

// Register - register new users by ws connection
func (s service) Register(conn io.ReadWriteCloser) WsUser {
	var messageChan = MessageChan{
		stopListenerChan:   make(chan struct{}, 1),
		messageHandlerChan: make(chan models.Response, handlerChanSize),
		messageFrameChan:   make(chan RawData, 1),
	}

	var pingPong = PingPong{
		pingPongTicker: time.NewTicker(pingTimeout * time.Second),
	}

	var user = WsUser{
		ID:              uuid.New().String(),
		io:              &sync.Mutex{},
		s:               &s,
		conn:            conn,
		wsReader:        wsutil.NewReader(conn, ws.StateServerSide),
		practices:       make([]string, 0),
		channelsHandler: make(map[string]*redis.PubSub),
		messageChan:     messageChan,
		pingPong:        pingPong,
	}

	go user.writer()
	go user.reader()

	user.writeNotice(user.ID, models.EventRegister, struct{}{})

	return user
}

// Publish events to ws chan
func (s service) Publish(event models.Event, data interface{}, keyFormat ...string) error {
	ID, err := uuid.NewUUID()
	if err != nil {
		logger.GetLog().Error("couldn't generate UUID", zap.Error(err))
		return err
	}
	resp := models.Response{
		ID:    ID.String(),
		Event: event,
		Body:  data,
	}
	jsonData, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	var channelFmt = updateChannel
	if event == models.EventSubscribe {
		channelFmt = channel
	}

	for i := range keyFormat {
		if err := s.rdb.Publish(fmt.Sprintf(channelFmt, keyFormat[i]), jsonData); err != nil {
			logger.GetLog().Error("data wasn't publish to rdb", zap.Error(err))
			return err
		}
	}
	return nil
}

// Receive method reads client data from ws connection.Should call it if wants to receive data from ws
func (u *WsUser) Receive() {
	for {
		msg, code, err := wsutil.ReadClientData(u.conn)
		if err != nil {
			u.Unsubscribe() // nolint:errcheck
			u.conn.Close()
			u.messageChan.stopListenerChan <- struct{}{}
			logger.GetLog().Error("couldn't read client data", zap.Error(err))
			return
		}
		u.messageChan.messageFrameChan <- RawData{
			msg:    msg,
			opCode: code,
		}
	}
}

// subscribe  implements subscribing to list of practices in redis
func (u *WsUser) subscribe(practicesID ...string) error {
	practices2sub := make([]string, 0)
	practices2subUpd := make([]string, 0)
	for i := range practicesID {
		practices2subUpd = append(practices2subUpd, fmt.Sprintf(updateChannel, practicesID[i]))
		userChannelsKey := fmt.Sprintf(appointmentList, practicesID[i])
		if ok, err := u.s.rdb.SIsMember(userChannelsKey, u.ID); !ok && err != nil {
			logger.GetLog().Error("is not a channel member", zap.String("user", u.ID), zap.Error(err))
			continue
		}
		if _, err := u.s.rdb.SAdd(userChannelsKey, u.ID); err != nil {
			logger.GetLog().Error("couldn't add user to list", zap.String("user", u.ID), zap.Error(err))
			return err
		}

		u.io.Lock()
		u.practices = append(u.practices, practicesID[i])
		u.io.Unlock()

		practices2sub = append(practices2sub, fmt.Sprintf(channel, practicesID[i]))
	}

	if errSubNew := u.doSubscribe(practices2sub...); errSubNew != nil {
		logger.GetLog().Error("couldn't subscribe to new practices", zap.String("user", u.ID), zap.Error(errSubNew))
		return errSubNew
	}

	if errSubUpd := u.doSubscribe(practices2subUpd...); errSubUpd != nil {
		logger.GetLog().Error("couldn't subscribe to upd practices", zap.String("user", u.ID), zap.Error(errSubUpd))
		return errSubUpd
	}

	return nil
}

// doSubscribe calling rdb to subscribe into redis and storing chan
func (u *WsUser) doSubscribe(practices ...string) error {
	if len(practices) == 0 {
		logger.GetLog().Error("list of practices is empty")
		return nil
	}
	// subscribe to practices channels in one request
	u.messageChan.messageNewAppointmentChan = u.s.rdb.Subscribe(practices...).Channel()

	return nil
}

// Unsubscribe from redis
func (u *WsUser) Unsubscribe() error {
	u.io.Lock()
	defer u.io.Unlock()
	for i := range u.practices {
		userChannelsKey := fmt.Sprintf(appointmentList, u.practices[i])
		if _, err := u.s.rdb.SIsMember(userChannelsKey, u.ID); err != nil {
			logger.GetLog().Error("sIsMember returned err", zap.Error(err), zap.String("by key", userChannelsKey))
			return err
		}
		if _, err := u.s.rdb.SRem(userChannelsKey, u.ID); err != nil {
			logger.GetLog().Error("couldn't remove channel key ", zap.Error(err), zap.String("by key", userChannelsKey))
			return err
		}
	}

	defer func() {
		u.messageChan.stopListenerChan <- struct{}{}
		u.practices = nil
	}()

	return nil
}

// writeNotice wrapper for sending messages
func (u *WsUser) writeNotice(id string, e models.Event, params interface{}) {
	msg := models.Response{
		ID:    id,
		Event: e,
		Body:  params,
	}
	u.messageChan.messageHandlerChan <- msg
}

// writer receiving data from channels for sending in web-socket connections
func (u *WsUser) writer() {
	w := wsutil.NewWriter(u.conn, ws.StateServerSide, ws.OpText)
	encoder := json.NewEncoder(w)

	for {
		select {
		case msg, ok := <-u.messageChan.messageHandlerChan:

			if !ok {
				u.messageChan.stopListenerChan <- struct{}{}
				logger.GetLog().Debug("write stopped")
				return
			}

			if err := encoder.Encode(msg); err != nil {
				logger.GetLog().Debug("couldn't encode message", zap.Error(err))
				continue
			}

			if err := w.Flush(); err != nil {
				logger.GetLog().Debug("couldn't flush a message", zap.Error(err))
				u.messageChan.stopListenerChan <- struct{}{}
				return
			}

		case <-u.messageChan.stopListenerChan:
			return
		case pingCheck, ok := <-u.pingPong.pingPongTicker.C:
			if !ok {
				u.messageChan.stopListenerChan <- struct{}{}
				return
			}
			u.pingPongCheck(pingCheck)
		}
	}
}

// pingPongCheck checking last ping time,a method is able to close web-socket connection
func (u *WsUser) pingPongCheck(pingCheck time.Time) {
	if time.Since(u.pingPong.lastPingSent).Seconds() > pingTimeout {
		lastReceived := u.pingPong.lastPongReceived
		if !lastReceived.IsZero() && (time.Since(u.pingPong.lastPongReceived).Seconds() > pingTimeout) {
			u.messageChan.stopListenerChan <- struct{}{}
			logger.GetLog().Error("close connection. ping wasn't received more then 1m")
		}
		u.conn.Write(ws.CompiledPing) // nolint:errcheck
		u.pingPong.lastPingSent = pingCheck
	}
}

// reader implements reading data from channels
func (u *WsUser) reader() { // nolint:gocyclo
	for {
		select {
		case msg, ok := <-u.messageChan.messageNewAppointmentChan:
			if err := u.sendToMessageChan(msg.Payload, ok); err != nil {
				continue
			}
		case msg, ok := <-u.messageChan.messageUpdateChan:
			if err := u.sendToMessageChan(msg.Payload, ok); err != nil {
				continue
			}

		case rawData, ok := <-u.messageChan.messageFrameChan:
			{
				if !ok {
					u.messageChan.stopListenerChan <- struct{}{}
					return
				}

				// raw data cases
				switch rawData.opCode {
				case ws.OpClose:
					u.Unsubscribe() // nolint:errcheck
					u.conn.Close()
					u.messageChan.stopListenerChan <- struct{}{}
					return
				case ws.OpPong:
					u.pingPong.lastPongReceived = time.Now()
					continue
				}

				//parse request and check token
				req, err := u.parseRequest(rawData)
				if err != nil {
					u.writeNotice("", models.EventError, errors.New("bad request"))
					continue
				}

				err = u.Auth(req)
				if err != nil {
					u.writeNotice(req.ID, models.EventError, err)
					continue
				}

				// request events
				if req.Event == models.EventSubscribe {
					if subscribeErr := u.eventSubscribe(req); subscribeErr != nil {
						u.writeNotice(req.ID, models.EventError, err)
						return
					}
				}
			}

		case <-u.messageChan.stopListenerChan:
			u.Unsubscribe() // nolint:errcheck
			u.conn.Close()
			return
		}
	}
}

func (u *WsUser) eventSubscribe(req models.Request) error {
	var (
		practicesRaw []interface{}
		ok           bool
	)

	if practicesRaw, ok = req.Body["practice_ids"].([]interface{}); !ok {
		return httperrors.NewBadRequestError(errors.New("couldn't parse subscribe body"))
	}

	practices := u.parseSubscribeBody(practicesRaw)

	if err := u.subscribe(practices...); err != nil {
		return httperrors.NewInternalServerError(err)
	}

	return nil
}

//util methods

//parseSubscribeBody parse  list of interfaces into string list
func (u *WsUser) parseSubscribeBody(practicesRaw []interface{}) []string {
	practices := make([]string, 0)
	for _, value := range practicesRaw {
		var practiceID string
		practiceID, ok := value.(string)
		if !ok {
			return practices
		}
		practices = append(practices, practiceID)
	}
	return practices
}

//parseRequest parse RawData into models.Request
func (u *WsUser) parseRequest(rawData RawData) (models.Request, error) {
	var req models.Request
	if err := json.Unmarshal(rawData.msg, &req); err != nil {
		return models.Request{}, httperrors.NewBadRequestError(err)
	}
	return req, nil
}

// sendToMessageChan send message to chan
func (u *WsUser) sendToMessageChan(payload string, ok bool) error {
	if !ok {
		u.messageChan.stopListenerChan <- struct{}{}
		return nil
	}
	var resp models.Response
	if err := json.Unmarshal([]byte(payload), &resp); err != nil {
		return err
	}
	u.messageChan.messageHandlerChan <- resp

	return nil
}

// Auth check if request has valid token
func (u *WsUser) Auth(req models.Request) httperrors.HTTPError {
	token, ok := req.Body["access_token"].(string)
	if !ok {
		logger.GetLog().Error("request body doesn't have access_token")
		return httperrors.NewBadRequestError(errors.New("can't parse subscribe request"))
	}
	jwtToken, err := u.ParseAuthorizationToken(token)
	if err != nil {
		logger.GetLog().Error("token is invalid", zap.String("token", token))
		return httperrors.NewUnauthorizedError(err)
	}

	claims, err := u.s.jwt.Validate(jwtToken)
	if err != nil {
		logger.GetLog().Error("token is invalid", zap.String("token", jwtToken))
		return httperrors.NewUnauthorizedError(err)
	}

	u.userRole = claims.UserRole
	u.userID = claims.UserID

	return nil
}

// ParseAuthorizationToken get jwt token from request
func (u *WsUser) ParseAuthorizationToken(token string) (string, error) {
	authSlice := strings.Split(strings.TrimSpace(token), " ")
	if len(authSlice) != 2 || !strings.EqualFold(authSlice[0], "Bearer") {
		return "", errors.New("invalid authorization header")
	}

	return authSlice[1], nil
}
