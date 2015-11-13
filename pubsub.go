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

type operation int

const (
	sub operation = iota
	subOnce
	pubBlock
	pubNonBlock
	unsub
	unsubAll
	closeTopic
	shutdown
)

// PubSub is a collection of topics.
type PubSub struct {
	cmdChan          chan cmd
	capacity         int
	default_pub_mode operation
}

type cmd struct {
	op     operation
	topics []string
	ch     chan interface{}
	msg    interface{}
}

// New creates a new PubSub and starts a goroutine for handling operations.
// The capacity of the channels created by Sub and SubOnce will be as specified.
func New(capacity int) *PubSub {
	ps := &PubSub{make(chan cmd), capacity, pubBlock}
	go ps.start()
	return ps
}

// NewNonBlock creates a new PubSub just like New but with non-blocking send operations
// This might cause messages to be lost, but guarantees Pub returns even if a subscriber
// stops collecting messages.
func NewNonBlocking(capacity int) *PubSub {
	ps := &PubSub{make(chan cmd), capacity, pubNonBlock}
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
func (ps *PubSub) SubOnce(topics ...string) chan interface{} {
	return ps.sub(subOnce, topics...)
}

func (ps *PubSub) sub(op operation, topics ...string) chan interface{} {
	ch := make(chan interface{}, ps.capacity)
	ps.cmdChan <- cmd{op: op, topics: topics, ch: ch}
	return ch
}

// AddSub adds subscriptions to an existing channel.
func (ps *PubSub) AddSub(ch chan interface{}, topics ...string) {
	ps.cmdChan <- cmd{op: sub, topics: topics, ch: ch}
}

// Pub publishes the given message to all subscribers of
// the specified topics.
func (ps *PubSub) Pub(msg interface{}, topics ...string) {
	ps.cmdChan <- cmd{op: ps.default_pub_mode, topics: topics, msg: msg}
}

// Pub2 works like Pub, but takes one additonal parameter
// @var ensure_dilvery If true, garantees message will not be lost. If false, guarantees system keeps running in case of error.
func (ps *PubSub) Pub2(ensure_dilvery bool, msg interface{}, topics ...string) {
	if ensure_dilvery {
		ps.cmdChan <- cmd{op: pubBlock, topics: topics, msg: msg}
	} else {
		ps.cmdChan <- cmd{op: pubNonBlock, topics: topics, msg: msg}
	}
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
// If a channel is subscribed to multiple topics, some of which is
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
		topics:    make(map[string]map[chan interface{}]bool),
		revTopics: make(map[chan interface{}]map[string]bool),
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
				reg.add(topic, cmd.ch, false)

			case subOnce:
				reg.add(topic, cmd.ch, true)

			case pubBlock:
				reg.sendBlock(topic, cmd.msg)

			case pubNonBlock:
				reg.sendNonBlock(topic, cmd.msg)

			case unsub:
				reg.remove(topic, cmd.ch)

			case closeTopic:
				reg.removeTopic(topic)
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
	topics    map[string]map[chan interface{}]bool
	revTopics map[chan interface{}]map[string]bool
}

func (reg *registry) add(topic string, ch chan interface{}, once bool) {
	if reg.topics[topic] == nil {
		reg.topics[topic] = make(map[chan interface{}]bool)
	}
	reg.topics[topic][ch] = once

	if reg.revTopics[ch] == nil {
		reg.revTopics[ch] = make(map[string]bool)
	}
	reg.revTopics[ch][topic] = true
}

func (reg *registry) sendBlock(topic string, msg interface{}) {
	for ch, once := range reg.topics[topic] {
		ch <- msg
		if once {
			for topic := range reg.revTopics[ch] {
				reg.remove(topic, ch)
			}
		}
	}
}

func (reg *registry) sendNonBlock(topic string, msg interface{}) {
	for ch, once := range reg.topics[topic] {
		select {
		case ch <- msg:
		default:
		}
		if once {
			for topic := range reg.revTopics[ch] {
				reg.remove(topic, ch)
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
	for topic := range reg.revTopics[ch] {
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

	if len(reg.topics[topic]) == 0 {
		delete(reg.topics, topic)
	}

	if len(reg.revTopics[ch]) == 0 {
		close(ch)
		delete(reg.revTopics, ch)
	}
}
