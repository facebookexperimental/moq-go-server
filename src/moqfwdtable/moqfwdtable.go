/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqfwdtable

import (
	"errors"
	"facebookexperimental/moq-go-server/moqhelpers"
	"facebookexperimental/moq-go-server/moqsession"
	"fmt"
	"sync"
)

type MoqFwdTable struct {
	sessions map[string]*moqsession.MoqSession

	// FilesLock Lock used to write / read files
	lock *sync.RWMutex
}

// New Creates a new moq forward table
func New() *MoqFwdTable {
	mft := MoqFwdTable{sessions: map[string]*moqsession.MoqSession{}, lock: new(sync.RWMutex)}

	return &mft
}

func (mft *MoqFwdTable) AddSession(session *moqsession.MoqSession) (err error) {
	mft.lock.Lock()
	defer mft.lock.Unlock()

	_, found := mft.sessions[session.UniqueName]
	if found {
		err = errors.New("We can NOT override a session")
		return
	}
	mft.sessions[session.UniqueName] = session

	return nil
}

func (mft *MoqFwdTable) RemoveSession(sessionName string) (err error) {
	mft.lock.Lock()
	defer mft.lock.Unlock()

	session, found := mft.sessions[sessionName]
	if found {
		delete(mft.sessions, sessionName)
		// Indicates sending thread to finish
		session.StopThreads()
	}
	if !found {
		err = errors.New(fmt.Sprintf("We could NOT find session to delete %s", sessionName))
	}

	return err
}

func (mft *MoqFwdTable) ReceivedObject(cacheKey string) (err error) {
	mft.lock.RLock()
	defer mft.lock.RUnlock()

	for _, session := range mft.sessions {
		if (session.Role == moqhelpers.MoqRoleSubscriber || session.Role == moqhelpers.MoqRoleBoth) && session.NeedsToBeDForwarded(cacheKey) {
			session.ReceivedObject(cacheKey)
		}
	}
	return
}

func (mft *MoqFwdTable) ForwardSubscribe(subscribe moqhelpers.MoqMessageSubscribe) (err error) {
	anyPublishers := false
	mft.lock.RLock()
	defer mft.lock.RUnlock()

	// Forward to local publishers
	for _, session := range mft.sessions {
		if session.Role == moqhelpers.MoqRolePublisher {
			if session.HasTrackNamespace(subscribe.TrackNamespace) {
				session.ForwardSubscribe(subscribe)
				anyPublishers = true
			}
		}
	}

	if !anyPublishers {
		// If not found locally forward to relays
		for _, session := range mft.sessions {
			if session.Role == moqhelpers.MoqRoleBoth {
				if session.HasTrackNamespace(subscribe.TrackNamespace) {
					session.ForwardSubscribe(subscribe)
					anyPublishers = true
				}
			}
		}
	}
	if !anyPublishers {
		err = errors.New(fmt.Sprintf("We could NOT find any publishers for TrackNamespace %s", subscribe.TrackNamespace))
	}

	return
}

func (mft *MoqFwdTable) ForwardSubscribeOk(subscribeOk moqhelpers.MoqMessageSubscribeOk) (err error) {
	anyUpdatedPublishers := false
	mft.lock.RLock()
	defer mft.lock.RUnlock()

	// TODO: Moqbug I need a way to identify the subscribe answer from publisher to source subscriber session
	// Here is sending OK to all subscribed
	for _, session := range mft.sessions {
		if session.Role == moqhelpers.MoqRoleSubscriber || session.Role == moqhelpers.MoqRoleBoth {
			updated := session.HasPendingTrackSubscriptionUpdate(subscribeOk.TrackNamespace, subscribeOk.TrackName, subscribeOk.TrackId, subscribeOk.Expires)
			if updated {
				session.ForwardSubscribeResponseOk(subscribeOk)
				anyUpdatedPublishers = true
			}
		}
	}

	if !anyUpdatedPublishers {
		err = errors.New(fmt.Sprintf("We could NOT find any publishers for %s", subscribeOk.TrackNamespace))
	}

	return
}

func (mft *MoqFwdTable) ForwardSubscribeError(subscribeError moqhelpers.MoqMessageSubscribeError) (err error) {
	anyDeletedPublishers := false
	mft.lock.RLock()
	defer mft.lock.RUnlock()

	// TODO: Moqbug I need a way to identify the subscribe answer from publisher to source subscriber session
	// Here is sending OK to all subscribed
	for _, session := range mft.sessions {
		if session.Role == moqhelpers.MoqRoleSubscriber || session.Role == moqhelpers.MoqRoleBoth {
			deleted := session.HasPendingTrackSubscriptionDelete(subscribeError.TrackNamespace, subscribeError.TrackName)
			if deleted {
				session.ForwardSubscribeResponseError(subscribeError)
				anyDeletedPublishers = true
			}
		}
	}

	if !anyDeletedPublishers {
		err = errors.New(fmt.Sprintf("We could NOT find any publishers for %s", subscribeError.TrackNamespace))
	}

	return
}
