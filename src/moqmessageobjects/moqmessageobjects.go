/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqmessageobjects

import (
	"errors"
	"fmt"
	"jordicenzano/moq-go-server/moqobject"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// File Definition of files
type MoqMessageObjects struct {
	dataMap map[string]*moqobject.MoqObject

	// FilesLock Lock used to write / read files
	mapLock *sync.RWMutex

	// Housekeeping thread channel
	cleanUpChannel chan bool
}

// New Creates a new mem files map
func New(housekeepingPeriodMs int64) *MoqMessageObjects {
	moqtObjs := MoqMessageObjects{dataMap: map[string]*moqobject.MoqObject{}, mapLock: new(sync.RWMutex), cleanUpChannel: make(chan bool)}

	if housekeepingPeriodMs > 0 {
		moqtObjs.startCleanUp(housekeepingPeriodMs)
	}

	return &moqtObjs
}

func (moqtObjs *MoqMessageObjects) Create(cacheKey string, objHeader moqobject.MoqObjectHeader, defObjExpirationS uint64) (moqObj *moqobject.MoqObject, err error) {
	moqtObjs.mapLock.Lock()
	defer moqtObjs.mapLock.Unlock()

	foundObj, found := moqtObjs.dataMap[cacheKey]
	if found && !foundObj.GetEof() {
		err = errors.New("We can NOT override on open object")
		return
	}

	moqObj = moqobject.New(objHeader, defObjExpirationS)
	moqtObjs.dataMap[cacheKey] = moqObj

	return
}

func (moqtObjs *MoqMessageObjects) Get(cacheKey string) (moqObjRet *moqobject.MoqObject, found bool) {
	moqtObjs.mapLock.RLock()
	defer moqtObjs.mapLock.RUnlock()

	moqObjRet, found = moqtObjs.dataMap[cacheKey]

	return
}

func (moqtObjs *MoqMessageObjects) Stop() {
	moqtObjs.stopCleanUp()
}

// Housekeeping

func (moqtObjs *MoqMessageObjects) startCleanUp(periodMs int64) {
	go moqtObjs.runCleanupEvery(periodMs, moqtObjs.cleanUpChannel)

	log.Info("Started clean up thread")
}

func (moqtObjs *MoqMessageObjects) stopCleanUp() {
	// Send finish signal
	moqtObjs.cleanUpChannel <- true

	// Wait to finish
	<-moqtObjs.cleanUpChannel

	log.Info("Stopped clean up thread")
}

func (moqtObjs *MoqMessageObjects) runCleanupEvery(periodMs int64, cleanUpChannelBidi chan bool) {
	timeCh := time.NewTicker(time.Millisecond * time.Duration(periodMs))
	exit := false

	for !exit {
		select {
		// Wait for the next tick
		case tm := <-timeCh.C:
			moqtObjs.cacheCleanUp(tm)

		case <-cleanUpChannelBidi:
			exit = true
		}
	}
	// Indicates finished
	cleanUpChannelBidi <- true

	log.Info("Exited clean up thread")
}

func (moqtObjs *MoqMessageObjects) cacheCleanUp(now time.Time) {
	objectsToDel := map[string]*moqobject.MoqObject{}

	// TODO: This is a brute force approach, optimization recommended

	moqtObjs.mapLock.Lock()
	defer moqtObjs.mapLock.Unlock()

	numStartElements := len(moqtObjs.dataMap)

	// Check for expired files
	for key, obj := range moqtObjs.dataMap {
		if obj.MaxAgeS >= 0 && obj.GetEof() {
			if obj.ReceivedAt.Add(time.Second * time.Duration(obj.MaxAgeS)).Before(now) {
				objectsToDel[key] = obj
			}
		}
	}
	// Delete expired files
	for keyToDel := range objectsToDel {
		// Delete from array
		delete(moqtObjs.dataMap, keyToDel)
		log.Info("CLEANUP MOQ object expired, deleted: ", keyToDel)
	}

	numEndElements := len(moqtObjs.dataMap)

	log.Info(fmt.Sprintf("Finished cleanup MOQ objects round expired. Elements at start: %d, elements at end: %d", numStartElements, numEndElements))
}
