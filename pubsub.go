// Copyright 2013, Chandra Sekar S.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the README.md file.

// Package pubsub implements a simple multi-topic pub-sub
// library.
//
// Topics must be strings and messages of any type can be
// published. A topic can have any number of subcribers and
// all of them receive messages published on the topic.
package pubsub

import "errors"

type operation int

type PubSubGoKickHandler func()

const (
	sub operation = iota
	subOnce
	subButKick
	addHandler
	pubBlock
	pubNonBlock
	unsub
	unsubAll
	closeTopic
	shutdown
)

// PubSub is a collection of topics.
type PubSub struct {
	cmdChan  chan cmd
	capacity int
}

type cmd struct {
	op           operation
	topics       []string
	ch           chan interface{}
	msg          interface{}
	errorhandler PubSubGoKickHandler
}

// New creates a new PubSub and starts a goroutine for handling operations.
// The capacity of the channels created by Sub and SubOnce will be as specified.
// The default ist he Blocking variant, since this is the original behaviour
func New(capacity int) *PubSub {
	ps := &PubSub{make(chan cmd), capacity}
	go ps.start()
	return ps
}

// Sub returns a channel on which messages published on any of
// the specified topics can be received.
func (ps *PubSub) Sub(topics ...string) chan interface{} {
	return ps.sub(sub, topics...)
}

// SubOnce is similar to Sub, but only the first message published, after subscription,
// on any of the specified topics can be received.
// Note that if you DO NOT CLEAR the channel in time, ALL PUBSUBs will be blocked
func (ps *PubSub) SubOnce(topics ...string) chan interface{} {
	return ps.sub(subOnce, topics...)
}

// Like Sub, but if you don't empty the channel queue fast enough and eventually block
// you will be kicked of the PubSub channel
// you can detect this by watching for a channel close signal
// additionally you can register a Handler per Channel (or the same for all channels)
func (ps *PubSub) SubButCloseChanIfBlock(topics ...string) chan interface{} {
	return ps.sub(subButKick, topics...)
}

func (ps *PubSub) sub(op operation, topics ...string) chan interface{} {
	ch := make(chan interface{}, ps.capacity)
	ps.cmdChan <- cmd{op: op, topics: topics, ch: ch}
	return ch
}

// Register a Handler to be called as goroutine if the given receiving channel should block
func (ps *PubSub) AddSubBlocksHandler(ch chan interface{}, gkh PubSubGoKickHandler) {
	ps.cmdChan <- cmd{op: addHandler, ch: ch, errorhandler: gkh}
}

// AddSub adds subscriptions to an existing channel.
func (ps *PubSub) AddSub(ch chan interface{}, topics ...string) {
	ps.cmdChan <- cmd{op: sub, topics: topics, ch: ch}
}

// Pub publishes the given message to all subscribers of the specified topics.
func (ps *PubSub) Pub(msg interface{}, topics ...string) {
	ps.cmdChan <- cmd{op: pubBlock, topics: topics, msg: msg}
}

// PubNonBlocking publishes the given message with Non-Blocking semantics
// If a receivers channel fill to capacity, any additional messages are dropped and the receiver in question is unsubscribed
// returns an error on congestion
func (ps *PubSub) PubNonBlocking(msg interface{}, topics ...string) error {
	select {
	case ps.cmdChan <- cmd{op: pubNonBlock, topics: topics, msg: msg}:
	default:
		return errors.New("PubSub Congestion: message lost")
	}
	return nil
}

// Unsub unsubscribes the given channel from the specified
// topics. If no topic is specified, it is unsubscribed
// from all topics.
func (ps *PubSub) Unsub(ch chan interface{}, topics ...string) {
	if len(topics) == 0 {
		ps.cmdChan <- cmd{op: unsubAll, ch: ch}
		return
	}

	ps.cmdChan <- cmd{op: unsub, topics: topics, ch: ch}
}

// Close closes all channels currently subscribed to the specified topics.
// If a channel is subscribed to multiple topics, some of which are
// not specified, it is not closed.
func (ps *PubSub) Close(topics ...string) {
	ps.cmdChan <- cmd{op: closeTopic, topics: topics}
}

// Shutdown closes all subscribed channels and terminates the goroutine.
func (ps *PubSub) Shutdown() {
	ps.cmdChan <- cmd{op: shutdown}
}

func (ps *PubSub) start() {
	reg := registry{
		topics:        make(map[string]map[chan interface{}]bool),
		revTopics:     make(map[chan interface{}]map[string]bool),
		blockHandlers: make(map[chan interface{}]PubSubGoKickHandler),
	}

loop:
	for cmd := range ps.cmdChan {
		if cmd.topics == nil {
			switch cmd.op {
			case unsubAll:
				reg.removeChannel(cmd.ch)

			case shutdown:
				break loop
			}

			continue loop
		}

		for _, topic := range cmd.topics {
			switch cmd.op {
			case sub:
				reg.add(topic, cmd.ch, false, false)

			case subButKick:
				reg.add(topic, cmd.ch, false, true)

			case subOnce:
				reg.add(topic, cmd.ch, true, false)

			case pubBlock, pubNonBlock:
				reg.send(topic, cmd.msg)

			case unsub:
				reg.remove(topic, cmd.ch)

			case closeTopic:
				reg.removeTopic(topic)

			case addHandler:
				reg.addHandler(cmd.errorhandler, cmd.ch)
			}
		}
	}

	for topic, chans := range reg.topics {
		for ch := range chans {
			reg.remove(topic, ch)
		}
	}
}

// registry maintains the current subscription state. It's not
// safe to access a registry from multiple goroutines simultaneously.
type registry struct {
	topics        map[string]map[chan interface{}]bool
	revTopics     map[chan interface{}]map[string]bool
	blockHandlers map[chan interface{}]PubSubGoKickHandler
}

func (reg *registry) addHandler(errorhandler PubSubGoKickHandler, ch chan interface{}) {
	if errorhandler == nil {
		return
	}
	reg.blockHandlers[ch] = errorhandler
}

func (reg *registry) add(topic string, ch chan interface{}, once bool, oktokick bool) {
	if reg.topics[topic] == nil {
		reg.topics[topic] = make(map[chan interface{}]bool)
	}
	reg.topics[topic][ch] = once

	if reg.revTopics[ch] == nil {
		reg.revTopics[ch] = make(map[string]bool)
	}
	reg.revTopics[ch][topic] = oktokick
}

func (reg *registry) send(topic string, msg interface{}) {
	for ch, once := range reg.topics[topic] {
		select {
		case ch <- msg: //all good
			if once {
				reg.removeChannel(ch)
			}
		default:
			oktokick := reg.revTopics[ch][topic]
			if !oktokick {
				ch <- msg //probably die in a fire, but user want's this. e.g. you might want to test your code with capacity==0
			}
			if handler, handler_is_set := reg.blockHandlers[ch]; handler_is_set {
				go handler()
			}
			if oktokick || once {
				reg.removeChannel(ch)
			}
		}
	}
}

func (reg *registry) removeTopic(topic string) {
	for ch := range reg.topics[topic] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) removeChannel(ch chan interface{}) {
	for topic, _ := range reg.revTopics[ch] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) remove(topic string, ch chan interface{}) {
	if _, ok := reg.topics[topic]; !ok {
		return
	}

	if _, ok := reg.topics[topic][ch]; !ok {
		return
	}

	delete(reg.topics[topic], ch)
	delete(reg.revTopics[ch], topic)
	delete(reg.blockHandlers, ch)

	if len(reg.topics[topic]) == 0 {
		delete(reg.topics, topic)
	}

	if len(reg.revTopics[ch]) == 0 {
		close(ch)
		delete(reg.revTopics, ch)
	}
}
