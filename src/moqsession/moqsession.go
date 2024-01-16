/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqsession

import (
	"errors"
	"facebookexperimental/moq-go-server/moqhelpers"
	"fmt"
	"strings"
	"sync"
	"time"
)

const MAX_PUBLISH_NAMESPACES_PER_SESSION = 256
const MAX_SUBSCRIBE_TRACKS_PER_SESSION = 256
const SUBSCRIBER_INTERNAL_QUEUE_SIZE = 1024 * 1024

type moqNamespaceInfo struct {
	AuthInfo       string
	trackNamespace string
}

type MoqSubscribeChannelMessage struct {
	moqhelpers.MoqMessageSubscribe
	stop bool
}

type noOp struct{}

type MoqSubscribeResponseChannelMessage struct {
	moqSubscribeResponse interface{}
	subscribeMessageType moqhelpers.MoqMessageType
	stop                 bool
}

type MoqMessageSubscribeExtended struct {
	moqhelpers.MoqMessageSubscribe
	trackId   uint64
	expires   uint64
	validated bool
}

type MoqSession struct {
	UniqueName string

	CreatedAt time.Time

	// Setup
	Version moqhelpers.MoqVersion

	// Role
	Role moqhelpers.MoqRole

	// Data for publishers or both
	// Namespaces, trackId -> trackName
	namespaces map[string]map[uint64]string

	// Channel use to forward subscribes
	channelSubscribe chan MoqSubscribeChannelMessage

	// Channel use to forward subscribes response (Ok/Err) messages
	channelSubscribeResponse chan MoqSubscribeResponseChannelMessage

	// Data for subscribers or both
	// Track info
	tracks map[string]MoqMessageSubscribeExtended
	// Channel notify new objects
	channelObject chan string

	lock *sync.RWMutex
}

func New(uniqueName string, version moqhelpers.MoqVersion, role moqhelpers.MoqRole) *MoqSession {
	now := time.Now()
	s := MoqSession{UniqueName: uniqueName, CreatedAt: now, Version: version, Role: role, namespaces: map[string]map[uint64]string{}, tracks: map[string]MoqMessageSubscribeExtended{}, channelObject: make(chan string, SUBSCRIBER_INTERNAL_QUEUE_SIZE), channelSubscribe: make(chan MoqSubscribeChannelMessage, SUBSCRIBER_INTERNAL_QUEUE_SIZE), channelSubscribeResponse: make(chan MoqSubscribeResponseChannelMessage, SUBSCRIBER_INTERNAL_QUEUE_SIZE), lock: new(sync.RWMutex)}

	return &s
}

func (s *MoqSession) AddTrackNamespace(announce moqhelpers.MoqMessageAnnounce) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.Role == moqhelpers.MoqRolePublisher && len(s.namespaces) > MAX_PUBLISH_NAMESPACES_PER_SESSION {
		return errors.New("Max publish namespaces per session reached, can NOT add a new track")
	}
	s.namespaces[announce.TrackNamespace] = map[uint64]string{}
	return nil
}

func (s *MoqSession) RemoveTrackNamespace(trackNamespace string) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, found := s.namespaces[trackNamespace]
	if found {
		delete(s.namespaces, trackNamespace)
	} else {
		err = errors.New(fmt.Sprintf("Could NOT find namespace %s to delete", trackNamespace))
	}
	return err
}

func (s *MoqSession) HasTrackNamespace(trackNamespace string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, found := s.namespaces[trackNamespace]
	return found
}

func (s *MoqSession) AddTrackInfo(trackNamespace string, trackName string, trackId uint64) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	namespaceInfo, found := s.namespaces[trackNamespace]
	if found {
		namespaceInfo[trackId] = trackName
	} else {
		err = errors.New(fmt.Sprintf("Could NOT find track empty namespace %s to add track: %s (%d)", trackNamespace, trackName, trackId))
	}
	return err
}

func (s *MoqSession) GetTrackInfo(trackId uint64) (found bool, trackNamespace string, trackName string) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for trackNamespaceItem, trackInfoItem := range s.namespaces {
		for trackIdItem, trackNameItem := range trackInfoItem {
			if trackIdItem == trackId {
				trackNamespace = trackNamespaceItem
				trackName = trackNameItem
				trackName = trackNameItem
				found = true
				return
			}
		}
	}
	return
}

func (s *MoqSession) NeedsToBeDForwarded(cacheKey string) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// Cachekey example: simplechat/foo/1/0 [trackNamespace/trackName/Group/Obj]

	cacheKeyItems := strings.Split(cacheKey, "/")
	if len(cacheKeyItems) >= 2 {
		cacheKeyTrackNamespace := cacheKeyItems[0]
		cacheKeyTrackName := cacheKeyItems[1]
		for k := range s.tracks {
			// k [trackNamespace/trackName]
			if k == cacheKeyTrackNamespace+"/"+cacheKeyTrackName {
				return true
			}
		}
	}
	return false
}

func (s *MoqSession) AddSubscribeRequest(subscribe moqhelpers.MoqMessageSubscribe) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.Role == moqhelpers.MoqRoleSubscriber && len(s.tracks) > MAX_SUBSCRIBE_TRACKS_PER_SESSION {
		return errors.New("Max subscribe tracks per session reached, can NOT add a new track")
	}

	moqSubscribeExt := MoqMessageSubscribeExtended{subscribe, 0, 0, false}
	s.tracks[subscribe.TrackNamespace+"/"+subscribe.TrackName] = moqSubscribeExt
	return nil
}

func (s *MoqSession) HasPendingTrackSubscriptionUpdate(trackNamespace string, trackName string, trackId uint64, expires uint64) (updated bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	subscribeExt, found := s.tracks[trackNamespace+"/"+trackName]
	if found {
		if !subscribeExt.validated {
			subscribeExt.validated = true
			subscribeExt.trackId = trackId
			subscribeExt.expires = expires

			updated = true
		}
	}
	return
}

func (s *MoqSession) HasPendingTrackSubscriptionDelete(trackNamespace string, trackName string) (deleted bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	keyStr := trackNamespace + "/" + trackName
	_, found := s.tracks[keyStr]
	if found {
		delete(s.tracks, keyStr)
		deleted = true
	}
	return
}

func (s *MoqSession) StopThreads() {
	s.ReceivedObject("")
	s.forwardSubscribeStop()
	s.forwardSubscribeResponseStop()
}

func (s *MoqSession) ReceivedObject(cacheKey string) {
	s.channelObject <- cacheKey
}

func (s *MoqSession) GetNewObject() string {
	return <-s.channelObject
}

func (s *MoqSession) ForwardSubscribe(subscribe moqhelpers.MoqMessageSubscribe) {
	subscribeMsg := MoqSubscribeChannelMessage{subscribe, false}

	s.channelSubscribe <- subscribeMsg
}

func (s *MoqSession) GetNewSubscribe() (subscribe moqhelpers.MoqMessageSubscribe, stop bool) {
	subscribeExt := <-s.channelSubscribe

	subscribe = subscribeExt.MoqMessageSubscribe
	stop = subscribeExt.stop

	return
}

func (s *MoqSession) forwardSubscribeStop() {
	subscribeStop := MoqSubscribeChannelMessage{moqhelpers.MoqMessageSubscribe{}, true}

	s.channelSubscribe <- subscribeStop
}

func (s *MoqSession) ForwardSubscribeResponseOk(subscribeOk moqhelpers.MoqMessageSubscribeOk) {
	subscribeOkMsg := MoqSubscribeResponseChannelMessage{subscribeOk, moqhelpers.MoqIdSubscribeOk, false}

	s.channelSubscribeResponse <- subscribeOkMsg
}

func (s *MoqSession) ForwardSubscribeResponseError(subscribeError moqhelpers.MoqMessageSubscribeError) {
	subscribeErrorMsg := MoqSubscribeResponseChannelMessage{subscribeError, moqhelpers.MoqIdSubscribeError, false}

	s.channelSubscribeResponse <- subscribeErrorMsg
}

func (s *MoqSession) GetNewSubscribeResponse() (moqSubscribeResponse interface{}, subscribeMessageType moqhelpers.MoqMessageType, stop bool) {
	subscribeResponseMsg := <-s.channelSubscribeResponse

	moqSubscribeResponse = subscribeResponseMsg.moqSubscribeResponse
	subscribeMessageType = subscribeResponseMsg.subscribeMessageType
	stop = subscribeResponseMsg.stop

	return
}

func (s *MoqSession) forwardSubscribeResponseStop() {
	subscribeResponseStop := MoqSubscribeResponseChannelMessage{noOp{}, moqhelpers.InternalId, true}

	s.channelSubscribeResponse <- subscribeResponseStop
}
