/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqconnectionmanagment

import (
	"context"
	"errors"
	"facebookexperimental/moq-go-server/moqfwdtable"
	"facebookexperimental/moq-go-server/moqhelpers"
	"facebookexperimental/moq-go-server/moqmessageobjects"
	"facebookexperimental/moq-go-server/moqobject"
	"facebookexperimental/moq-go-server/moqsession"
	"fmt"
	"strconv"

	"github.com/quic-go/webtransport-go"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func MoqConnectionManagment(ctx context.Context, session *webtransport.Session, namespace string, moqtFwdTable *moqfwdtable.MoqFwdTable, objects *moqmessageobjects.MoqMessageObjects, objExpMs uint64) {

	// Accept bidirectional streams (control stream)
	stream, err := session.AcceptStream(ctx)
	if err != nil {
		log.Error(fmt.Sprintf("%s - Accepting bidirectional CONTROL stream. Err: %v", namespace, err))
		return
	}

	moqMsg, moqMsgType, moqMsgErr := moqhelpers.ReceiveMessage(stream)
	if moqMsgErr != nil {
		log.Error(fmt.Sprintf("%s - Receiving client SETUP message. Err: %v", namespace, moqMsgErr))
		terminateSessionWithError(session, moqhelpers.MoqError{ErrCode: moqhelpers.ErrorGeneric, ErrMsg: "Receiving SETUP message"})
		return
	}
	moqSetup, moqSetUpConv := moqMsg.(moqhelpers.MoqMessageSetup)
	if moqMsgType != moqhelpers.MoqIdMessageClientSetup || !moqSetUpConv {
		errStr := fmt.Sprintf("%s - Expecting client SETUP message. Received %d", namespace, moqMsgType)
		log.Error(errStr)
		terminateSessionWithError(session, moqhelpers.MoqError{ErrCode: moqhelpers.ErrorProtocolViolation, ErrMsg: "Not received SETUP message"})
		return
	}
	log.Info(fmt.Sprintf("%s - Received client SETUP %v", namespace, moqSetup))

	moqSetupResponse, errMoqCreateSetup := moqhelpers.CreateSetupResponse(moqSetup)
	if errMoqCreateSetup != nil {
		log.Error(fmt.Sprintf("%s - Processing client SETUP. Err: %v", namespace, errMoqCreateSetup))
		terminateSessionWithError(session, moqhelpers.MoqError{ErrCode: moqhelpers.ErrorProtocolViolation, ErrMsg: "Processing SETUP message"})
		return
	}

	if moqSetup.Role != moqhelpers.MoqRolePublisher && moqSetup.Role != moqhelpers.MoqRoleSubscriber {
		errStr := fmt.Sprintf("%s - Error invalid session type %d", namespace, moqSetup.Role)
		log.Error(errStr)
		terminateSessionWithError(session, moqhelpers.MoqError{ErrCode: moqhelpers.ErrorProtocolViolation, ErrMsg: "Invalid session type"})
		return
	}

	moqSession := moqsession.New(namespace+"/"+uuid.New().String(), moqSetupResponse.Version, moqSetup.Role)
	errAddSession := moqtFwdTable.AddSession(moqSession)
	if errAddSession != nil {
		log.Error(fmt.Sprintf("%s - Error adding session %s. Err: %v", moqSession.UniqueName, moqSession.UniqueName, errAddSession))
		terminateSessionWithError(session, moqhelpers.MoqError{ErrCode: moqhelpers.ErrorGeneric, ErrMsg: "Adding session"})
		return
	}

	if moqSetup.Role == moqhelpers.MoqRolePublisher {
		// They will exit when session finishes
		go startListeningObjects(session, moqSession, moqtFwdTable, objects, objExpMs)
		go startForwardSubscribes(stream, moqSession)
	} else if moqSetup.Role == moqhelpers.MoqRoleSubscriber {
		// It will exit when session finishes
		go startForwardingObjects(session, moqSession, objects)
		go startForwardSubscribeResponses(stream, moqSession)
	}

	errorSessionMoq := moqhelpers.MoqError{}
	errMoqTxSetup := moqhelpers.SendServerSetup(stream, moqSetupResponse)
	if errMoqTxSetup != nil {
		log.Error(fmt.Sprintf("%s - Sending server SETUP message. Err: %v", moqSession.UniqueName, errMoqTxSetup))
		errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
		errorSessionMoq.ErrMsg = "Error sending server SETUP"
	}
	log.Info(fmt.Sprintf("%s - Sent server SETUP %v", moqSession.UniqueName, moqSetupResponse))

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		// Process messages in the control loop
		for {
			moqMsg, moqMsgType, moqMsgErr := moqhelpers.ReceiveMessage(stream)
			//TODO: Check if session has closed successfully
			if moqMsgErr != nil {
				log.Error(fmt.Sprintf("%s - Receiving message. Err: %v", moqSession.UniqueName, moqMsgErr))
				errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
				errorSessionMoq.ErrMsg = "Error receiving message"
				break
			}
			if moqMsgType == moqhelpers.MoqIdMessageAnnounce {
				errorSessionMoq = processAnnounce(moqMsg, stream, moqSession)
				if errorSessionMoq.ErrCode != moqhelpers.NoError {
					break
				}
			} else if moqMsgType == moqhelpers.MoqIdSubscribe {
				errorSessionMoq = processSubscribe(moqMsg, stream, moqSession, moqtFwdTable)
				if errorSessionMoq.ErrCode != moqhelpers.NoError {
					break
				}
			} else if moqMsgType == moqhelpers.MoqIdSubscribeOk {
				errorSessionMoq = processSubscribeOk(moqMsg, stream, moqSession, moqtFwdTable)
				if errorSessionMoq.ErrCode != moqhelpers.NoError {
					break
				}
			} else if moqMsgType == moqhelpers.MoqIdSubscribeError {
				errorSessionMoq = processSubscribeError(moqMsg, stream, moqSession, moqtFwdTable)
				if errorSessionMoq.ErrCode != moqhelpers.NoError {
					break
				}
			} else {
				//TODO: Process other messages (such as errors)
				log.Error(fmt.Sprintf("%s - Non expected message received %d", moqSession.UniqueName, moqMsgType))
			}
		}
	}

	errRemoveSession := moqtFwdTable.RemoveSession(moqSession.UniqueName)
	if errRemoveSession != nil {
		log.Error(fmt.Sprintf("%s - Error removing session %s", moqSession.UniqueName, moqSession.UniqueName))
	}

	if errorSessionMoq.ErrCode != moqhelpers.NoError {
		terminateSessionWithError(session, errorSessionMoq)
	}
}

func terminateSessionWithError(session *webtransport.Session, errMoq moqhelpers.MoqError) {
	session.CloseWithError(webtransport.SessionErrorCode(errMoq.ErrCode), errMoq.ErrMsg)
}

func createObjectCacheKey(trackNamespace string, trackName string, moqObjectHeader moqobject.MoqObjectHeader) string {
	return trackNamespace + "/" + trackName + "/" + strconv.FormatUint(moqObjectHeader.GroupSequence, 10) + "/" + strconv.FormatUint(moqObjectHeader.ObjectSequence, 10)
}

func processAnnounce(moqMsg interface{}, stream webtransport.Stream, moqSession *moqsession.MoqSession) (errorSessionMoq moqhelpers.MoqError) {
	moqAnnounceError := moqhelpers.MoqMessageAnnounceError{}

	moqAnnounce, moqAnnounceConv := moqMsg.(moqhelpers.MoqMessageAnnounce)
	if !moqAnnounceConv {
		// Break session
		errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
		errorSessionMoq.ErrMsg = "Error casting ANNOUNCE"
		log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))
	} else {
		log.Info(fmt.Sprintf("%s - Received ANNOUNCE message %v", moqSession.UniqueName, moqAnnounce))
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		if moqSession.Role != moqhelpers.MoqRolePublisher {
			// Break session
			errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
			errorSessionMoq.ErrMsg = "Error received ANNOUNCE from NON publisher"
			log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))
		}
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		errAddAnnounceTrack := moqSession.AddTrackNamespace(moqAnnounce)
		if errAddAnnounceTrack != nil {
			// Announce error
			moqAnnounceError = moqhelpers.MoqMessageAnnounceError{ErrCode: moqhelpers.ErrorAnnounceAddingTrack, ErrMsg: "Error Adding new track on ANNOUNCE"}
			log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, moqAnnounceError.ErrMsg, errAddAnnounceTrack))
		}

		if errorSessionMoq.ErrCode == moqhelpers.NoError {
			// Session NOT broken
			if moqAnnounceError.ErrCode == moqhelpers.NoErrorAnnounce {
				// Send announce OK
				moqAnnounceOk := moqhelpers.CreateAnnounceOK(moqAnnounce)
				errMoqTxAnnounceOk := moqhelpers.SendAnnounceOK(stream, moqAnnounceOk)
				if errMoqTxAnnounceOk != nil {
					// Break session
					errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
					errorSessionMoq.ErrMsg = "Error sending ANNOUNCE OK"
					log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, errorSessionMoq.ErrMsg, errMoqTxAnnounceOk))
				} else {
					log.Info(fmt.Sprintf("%s - Sent ANNOUNCE OK message %v", moqSession.UniqueName, moqAnnounceOk))
				}
			} else {
				// Send announce Error
				errMoqTxAnnounceError := moqhelpers.SendAnnounceError(stream, moqAnnounceError)
				if errMoqTxAnnounceError != nil {
					// Break session
					errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
					errorSessionMoq.ErrMsg = "Error sending ANNOUNCE error"
					log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, errorSessionMoq.ErrMsg, errMoqTxAnnounceError))
				} else {
					log.Info(fmt.Sprintf("%s - Sent ANNOUNCE error message %v", moqSession.UniqueName, moqAnnounceError))
				}
			}
		}
	}

	return
}

func processSubscribe(moqMsg interface{}, stream webtransport.Stream, moqSession *moqsession.MoqSession, moqtFwdTable *moqfwdtable.MoqFwdTable) (errorSessionMoq moqhelpers.MoqError) {
	moqSubscribeError := moqhelpers.MoqMessageSubscribeError{}

	moqSubscribe, moqSubscribeConv := moqMsg.(moqhelpers.MoqMessageSubscribe)
	if !moqSubscribeConv {
		// Break session
		errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
		errorSessionMoq.ErrMsg = "Error casting SUBSCRIBE"
		log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))

	} else {
		log.Info(fmt.Sprintf("%s - Received SUBSCRIBE message %v", moqSession.UniqueName, moqSubscribe))
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		if moqSession.Role != moqhelpers.MoqRoleSubscriber {
			// Break session
			errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
			errorSessionMoq.ErrMsg = "Error received SUBSCRIBE from NON subscriber"
			log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))
		}
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		errAddingSubscribeReq := moqSession.AddSubscribeRequest(moqSubscribe)
		if errAddingSubscribeReq != nil {
			moqSubscribeError = moqhelpers.MoqMessageSubscribeError{ErrCode: moqhelpers.ErrorSubscribeAddingTrack, ErrMsg: "Error Adding new subscription on SUBSCRIBE"}
			log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, moqSubscribeError.ErrMsg, errAddingSubscribeReq))
		}
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		// Session NOT broken
		if moqSubscribeError.ErrCode == moqhelpers.NoErrorSubscribe {
			// Forward every subscribe to publishers of that stream
			errForwardSubscribe := moqtFwdTable.ForwardSubscribe(moqSubscribe)
			if errForwardSubscribe != nil {
				moqSubscribeError = moqhelpers.MoqMessageSubscribeError{ErrCode: moqhelpers.ErrorSubscribeNoPublishers, ErrMsg: errForwardSubscribe.Error()}
			}
		}

		// Send subscribe error if needed
		if moqSubscribeError.ErrCode != moqhelpers.NoErrorSubscribe {
			errMoqTxSubscribeError := moqhelpers.SendSubscribeError(stream, moqSubscribeError)
			if errMoqTxSubscribeError != nil {
				// Break session
				errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
				errorSessionMoq.ErrMsg = "Error sending SUBSCRIBE error"
				log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, errorSessionMoq.ErrMsg, errMoqTxSubscribeError))
			} else {
				log.Info(fmt.Sprintf("%s - Sent SUBSCRIBE error message %v", moqSession.UniqueName, moqSubscribeError))
			}
		}
	}
	return
}

func processSubscribeOk(moqMsg interface{}, stream webtransport.Stream, moqSession *moqsession.MoqSession, moqtFwdTable *moqfwdtable.MoqFwdTable) (errorSessionMoq moqhelpers.MoqError) {
	moqSubscribeOk, moqSubscribeConv := moqMsg.(moqhelpers.MoqMessageSubscribeOk)
	if !moqSubscribeConv {
		// Break session
		errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
		errorSessionMoq.ErrMsg = "Error casting SUBSCRIBE OK"
		log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))
	} else {
		log.Info(fmt.Sprintf("%s - Received SUBSCRIBE OK message %v", moqSession.UniqueName, moqSubscribeOk))
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		if moqSession.Role != moqhelpers.MoqRolePublisher {
			// Break session
			errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
			errorSessionMoq.ErrMsg = "Error received SUBSCRIBE OK from NON publisher"
			log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))
		}
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		// Forward and add those subscriptions to sessions
		errForwardSubscribe := moqtFwdTable.ForwardSubscribeOk(moqSubscribeOk)
		if errForwardSubscribe != nil {
			// Break session
			errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
			errorSessionMoq.ErrMsg = errForwardSubscribe.Error()
			log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, errorSessionMoq.ErrMsg, errForwardSubscribe))
		}
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		// Add track info to current session
		errAddingTrackInfo := moqSession.AddTrackInfo(moqSubscribeOk.TrackNamespace, moqSubscribeOk.TrackName, moqSubscribeOk.TrackId)
		if errAddingTrackInfo != nil {
			// Break session
			errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
			errorSessionMoq.ErrMsg = errAddingTrackInfo.Error()
			log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, errorSessionMoq.ErrMsg, errAddingTrackInfo))
		}
	}

	return
}

func processSubscribeError(moqMsg interface{}, stream webtransport.Stream, moqSession *moqsession.MoqSession, moqtFwdTable *moqfwdtable.MoqFwdTable) (errorSessionMoq moqhelpers.MoqError) {
	moqSubscribeError, moqSubscribeConv := moqMsg.(moqhelpers.MoqMessageSubscribeError)
	if !moqSubscribeConv {
		// Break session
		errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
		errorSessionMoq.ErrMsg = "Error casting SUBSCRIBE Error"
		log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))

	} else {
		log.Info(fmt.Sprintf("%s - Received SUBSCRIBE Error message %v", moqSession.UniqueName, moqSubscribeError))
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		if moqSession.Role != moqhelpers.MoqRolePublisher {
			// Break session
			errorSessionMoq.ErrCode = moqhelpers.ErrorProtocolViolation
			errorSessionMoq.ErrMsg = "Error received SUBSCRIBE Error from NON publisher"
			log.Error(fmt.Sprintf("%s - %s", moqSession.UniqueName, errorSessionMoq.ErrMsg))
		}
	}

	if errorSessionMoq.ErrCode == moqhelpers.NoError {
		// TODO JOC: Forward to subscribers
		errForwardSubscribe := moqtFwdTable.ForwardSubscribeError(moqSubscribeError)
		if errForwardSubscribe != nil {
			// Break session
			errorSessionMoq.ErrCode = moqhelpers.ErrorGeneric
			errorSessionMoq.ErrMsg = errForwardSubscribe.Error()
			log.Error(fmt.Sprintf("%s - %s. Err: %v", moqSession.UniqueName, errorSessionMoq.ErrMsg, errForwardSubscribe))
		}
	}
	return
}

// Thread for publisher (forward subscribes)

func startForwardSubscribes(stream webtransport.Stream, moqSession *moqsession.MoqSession) {
	bExit := false
	for bExit == false {
		// Get next object cache key
		fwdSubscribe, stop := moqSession.GetNewSubscribe()
		if stop {
			bExit = true
		} else {
			// TODO we need to add mutex here
			errSendSubscribe := moqhelpers.SendSubscribe(stream, fwdSubscribe)
			if errSendSubscribe != nil {
				log.Error(fmt.Sprintf("%s - Forwarding SUBSCRIBE. Err: %v", moqSession.UniqueName, fwdSubscribe))
			} else {
				log.Info(fmt.Sprintf("%s - Forwarded SUBSCRIBE message %v", moqSession.UniqueName, fwdSubscribe))
			}
		}
	}

	log.Info(fmt.Sprintf("%s(-) - Exit Forwarding subscribes thread", moqSession.UniqueName))
}

// Thread for subscribers (forward subscribes responses)

func startForwardSubscribeResponses(stream webtransport.Stream, moqSession *moqsession.MoqSession) {
	bExit := false
	for bExit == false {
		// Get next object cache key
		subscribeResp, subscribeRespType, stop := moqSession.GetNewSubscribeResponse()
		if stop {
			bExit = true
		} else {
			// TODO we need to add mutex here
			var errSendSubscribe error
			if subscribeRespType == moqhelpers.MoqIdSubscribeOk {
				errSendSubscribe = moqhelpers.SendSubscribeOk(stream, subscribeResp.(moqhelpers.MoqMessageSubscribeOk))
			} else if subscribeRespType == moqhelpers.MoqIdMessageAnnounceError {
				errSendSubscribe = moqhelpers.SendSubscribeError(stream, subscribeResp.(moqhelpers.MoqMessageSubscribeError))
			} else {
				errSendSubscribe = errors.New(fmt.Sprintf("We can NOT forward this message type %d as subscribe response", subscribeRespType))
			}
			if errSendSubscribe != nil {
				log.Error(fmt.Sprintf("%s - Forwarding SUBSCRIBE. Err: %v", moqSession.UniqueName, subscribeResp))
			} else {
				log.Info(fmt.Sprintf("%s - Forwarded SUBSCRIBE response message %v", moqSession.UniqueName, subscribeResp))
			}
		}
	}

	log.Info(fmt.Sprintf("%s(-) - Exit Forwarding subscribes thread", moqSession.UniqueName))
}

// Thread for publisher (receive objects)

func startListeningObjects(session *webtransport.Session, moqSession *moqsession.MoqSession, moqtFwdTable *moqfwdtable.MoqFwdTable, objects *moqmessageobjects.MoqMessageObjects, objExpMs uint64) {
	for {
		uniStream, errAccUni := session.AcceptUniStream(session.Context())
		if errAccUni != nil {
			log.Error(fmt.Sprintf("%s - Session closed, not accepting more uni streams: %v", moqSession.UniqueName, errAccUni))
			break
		}
		log.Info(fmt.Sprintf("%s(%v) - Accepting incoming uni stream", moqSession.UniqueName, uniStream.StreamID()))

		go func(uniStream *webtransport.ReceiveStream, session *webtransport.Session, moqtFwdTable *moqfwdtable.MoqFwdTable) {
			moqMsg, moqMsgType, moqMsgErr := moqhelpers.ReceiveMessage(*uniStream)
			if moqMsgErr != nil {
				log.Error(fmt.Sprintf("%s - Receiving OBJECT message. Err: %v", moqSession.UniqueName, moqMsgErr))
				return
			}

			// TODO: Assuming object per QUIC stream

			moqObjHeader, moqObjHeaderConv := moqMsg.(moqobject.MoqObjectHeader)
			if moqMsgType != moqhelpers.MoqIdMessageObject || !moqObjHeaderConv {
				log.Error(fmt.Sprintf("%s - Expecting OBJECT message. Received %d", moqSession.UniqueName, moqMsgType))
				return
			}

			// Validate object
			foundTrack, trackNamespace, trackName := moqSession.GetTrackInfo(moqObjHeader.TrackId)
			if !foundTrack {
				log.Error(fmt.Sprintf("%s - TrackId %d, is NOT in this publishing session", moqSession.UniqueName, moqObjHeader.TrackId))
				return
			}

			// Create cache key
			cacheKey := createObjectCacheKey(trackNamespace, trackName, moqObjHeader)
			moqObj, errAddingMoqObj := objects.Create(cacheKey, moqObjHeader, objExpMs/1000)
			if errAddingMoqObj != nil {
				log.Error(fmt.Sprintf("%s(%v) - Received obj error, key: %s, Obj header: %s. Err: %v", moqSession.UniqueName, (*uniStream).StreamID(), cacheKey, moqObjHeader.GetDebugStr(), errAddingMoqObj))
			} else {
				log.Info(fmt.Sprintf("%s(%v) - Received obj header, key: %s, Obj: %s", moqSession.UniqueName, (*uniStream).StreamID(), cacheKey, moqObjHeader.GetDebugStr()))
			}

			// Notify new cache key
			moqtFwdTable.ReceivedObject(cacheKey)

			errObjPayload := moqhelpers.ReadObjPayloadToEOS(*uniStream, moqObj)
			if errObjPayload != nil {
				log.Error(fmt.Sprintf("%s(%v) - Error receiving obj payload. Err: %v", moqSession.UniqueName, (*uniStream).StreamID(), errObjPayload))
				return
			}
			log.Info(fmt.Sprintf("%s(%v) - Received obj, Obj: %s", moqSession.UniqueName, (*uniStream).StreamID(), moqObj.GetDebugStr()))

		}(&uniStream, session, moqtFwdTable)
	}
	log.Info(fmt.Sprintf("%s(-) - Exit ListeningObjects thread", moqSession.UniqueName))

	return
}

func startForwardingObjects(session *webtransport.Session, moqSession *moqsession.MoqSession, objects *moqmessageobjects.MoqMessageObjects) {
	bExit := false
	for bExit == false {
		// Get next object cache key
		cacheKey := moqSession.GetNewObject()
		if cacheKey == "" {
			bExit = true
		} else {
			moqObj, found := objects.Get(cacheKey)
			if !found {
				log.Error(fmt.Sprintf("%s - Not found OBJECT key %s in cache", moqSession.UniqueName, cacheKey))
			} else {
				go func(moqObj *moqobject.MoqObject, session *webtransport.Session, moqSession *moqsession.MoqSession) {
					sUni, errOpenStream := session.OpenUniStreamSync(session.Context())
					if errOpenStream != nil {
						log.Error(fmt.Sprintf("%s(-) - Opening stream to send OBJECT %s", moqSession.UniqueName, moqObj.GetDebugStr()))
					} else {
						log.Info(fmt.Sprintf("%s(%v) - Sending OBJECT %s", moqSession.UniqueName, sUni.StreamID(), moqObj.GetDebugStr()))
						errSendObj := moqhelpers.SendObject(sUni, moqObj)
						if errSendObj != nil {
							log.Error(fmt.Sprintf("%s(%v) - Sending OBJECT %s. Err: %v", moqSession.UniqueName, sUni.StreamID(), moqObj.GetDebugStr(), errSendObj))
						} else {
							log.Info(fmt.Sprintf("%s(%v) - Sent OBJECT %s", moqSession.UniqueName, sUni.StreamID(), moqObj.GetDebugStr()))
						}
						sUni.Close()
					}
				}(moqObj, session, moqSession)
			}
		}
	}

	log.Info(fmt.Sprintf("%s(-) - Exit Forwarding Objects thread", moqSession.UniqueName))

	return
}
