/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqhelpers

import (
	"errors"
	"fmt"
	"io"
	"jordicenzano/moq-go-server/moqhelpers/quichelpers"
	"jordicenzano/moq-go-server/moqobject"

	"golang.org/x/exp/slices"
)

const READ_BLOCK_SIZE_BYTES = 1024

const MAX_PROTOCOL_VERSIONS = 10
const MAX_PARAMS = 256
const MOQ_MAX_STRING_LENGTH = 1024

type MoqVersion uint

const (
	MoqVersionNotSet  MoqVersion = 0
	MoqVersionDraft00 MoqVersion = 0xff00
	MoqVersionDraft01 MoqVersion = 0xff000001
)

const MOQ_SUPPORTED_VERSION = MoqVersionDraft01

type MoqParams uint

const (
	MoqParamsRole              MoqParams = 0x0
	MoqParamsPath              MoqParams = 0x1
	MoqParamsAuthorizationInfo MoqParams = 0x2
)

type MoqRole uint

const (
	MoqRoleNotSet     MoqRole = 0
	MoqRolePublisher  MoqRole = 1
	MoqRoleSubscriber MoqRole = 2
	MoqRoleBoth       MoqRole = 3
)

type MoqLocationType uint

const (
	MoqLocationTypeNone             MoqLocationType = 0x0
	MoqLocationTypeAbsolute         MoqLocationType = 0x1
	MoqLocationTypeRelativePrevious MoqLocationType = 0x2
	MoqLocationTypeRelativeNext     MoqLocationType = 0x3
)

type MoqLocation struct {
	Type  MoqLocationType
	Value uint64
}

type MoqMessageType uint

const (
	MoqIdMessageObject        MoqMessageType = 0x0
	MoqIdMessageClientSetup   MoqMessageType = 0x40
	MoqIdMessageServerSetup   MoqMessageType = 0x41
	MoqIdSubscribe            MoqMessageType = 0x3
	MoqIdSubscribeOk          MoqMessageType = 0x4
	MoqIdSubscribeError       MoqMessageType = 0x5
	MoqIdMessageAnnounce      MoqMessageType = 0x6
	MoqIdMessageAnnounceOk    MoqMessageType = 0x7
	MoqIdMessageAnnounceError MoqMessageType = 0x8
	MoqIdMessageUnAnnounce    MoqMessageType = 0x9

	InternalId MoqMessageType = 0xffff
)

// MOQT messages

// Setup
type MoqMessageSetup struct {
	SupportedClientVersions []MoqVersion
	Role                    MoqRole
}

type MoqMessageSetupResponse struct {
	Version MoqVersion
	Role    MoqRole
}

// MOQT Errors

type MoqErrorCode uint64

const (
	NoError                MoqErrorCode = 0x0
	ErrorGeneric           MoqErrorCode = 0x1
	ErrorUnauthorized      MoqErrorCode = 0x2
	ErrorProtocolViolation MoqErrorCode = 0x3
	ErrorGoAwayTimeout     MoqErrorCode = 0x10
)

type MoqError struct {
	ErrCode MoqErrorCode
	ErrMsg  string
}

// Announce

type MoqMessageAnnounce struct {
	TrackNamespace string
	AuthInfo       string
}

type MoqMessageAnnounceOk struct {
	TrackNamespace string
}

type MoqErrorCodeAnnounce uint64

const (
	NoErrorAnnounce          MoqErrorCodeAnnounce = 0x0
	ErrorAnnounceGeneric     MoqErrorCodeAnnounce = 0x1
	ErrorAnnounceAddingTrack MoqErrorCodeAnnounce = 0x2
)

type MoqMessageAnnounceError struct {
	TrackNamespace string
	ErrCode        MoqErrorCodeAnnounce
	ErrMsg         string
}

// Subscribe

type MoqMessageSubscribe struct {
	TrackNamespace string
	TrackName      string
	StartGroup     MoqLocation
	StartObject    MoqLocation
	EndGroup       MoqLocation
	EndObject      MoqLocation
	AuthInfo       string
}

type MoqMessageSubscribeOk struct {
	TrackNamespace string
	TrackName      string
	TrackId        uint64
	Expires        uint64
}

type MoqErrorCodeSubscribe uint64

const (
	NoErrorSubscribe           MoqErrorCodeSubscribe = 0x0
	ErrorSubscribeGeneric      MoqErrorCodeSubscribe = 0x1
	ErrorSubscribeAddingTrack  MoqErrorCodeSubscribe = 0x2
	ErrorSubscribeNoPublishers MoqErrorCodeSubscribe = 0x3
)

type MoqMessageSubscribeError struct {
	TrackNamespace string
	TrackName      string
	ErrCode        MoqErrorCodeSubscribe
	ErrMsg         string
}

func CreateAnnounceOK(moqAnnounce MoqMessageAnnounce) (moqAnnounceOk MoqMessageAnnounceOk) {
	moqAnnounceOk.TrackNamespace = moqAnnounce.TrackNamespace

	return
}

func CreateSetupResponse(moqSetup MoqMessageSetup) (moqSetupResponse MoqMessageSetupResponse, err error) {
	if !slices.Contains(moqSetup.SupportedClientVersions, MOQ_SUPPORTED_VERSION) {
		err = errors.New(fmt.Sprintf("MOQ SETUP not supported version. Offered: %v, supported: %d", moqSetup.SupportedClientVersions, MOQ_SUPPORTED_VERSION))
		return
	}

	if moqSetup.Role == MoqRolePublisher {
		moqSetupResponse.Role = MoqRoleSubscriber
	} else if moqSetup.Role == MoqRoleSubscriber {
		moqSetupResponse.Role = MoqRolePublisher
	} else {
		err = errors.New("MOQ SETUP only publisher or subscriber connections supported for now")
		return
	}

	moqSetupResponse.Version = MOQ_SUPPORTED_VERSION

	return
}

func ReceiveMessage(stream quichelpers.IWtReadableStream) (moqMessage interface{}, moqMessageType MoqMessageType, err error) {
	msgType, errMsgType := quichelpers.ReadVarint(stream)
	if errMsgType != nil {
		err = errors.New(fmt.Sprintf("MOQ reading message type, err: %v", errMsgType))
		return
	}
	moqMessageType = MoqMessageType(msgType)

	if msgType == uint64(MoqIdMessageObject) {
		moqMessage, err = receiveObjectHeader(stream)
	} else if msgType == uint64(MoqIdMessageClientSetup) {
		moqMessage, err = receiveSetUp(stream)
	} else if msgType == uint64(MoqIdMessageAnnounce) {
		moqMessage, err = receiveAnnounce(stream)
	} else if msgType == uint64(MoqIdSubscribe) {
		moqMessage, err = receiveSubscribe(stream)
	} else if msgType == uint64(MoqIdSubscribeOk) {
		moqMessage, err = receiveSubscribeOk(stream)
	} else {
		err = errors.New(fmt.Sprintf("MOQ not supported message type %d", msgType))
	}
	return
}

func receiveSubscribeOk(stream quichelpers.IWtReadableStream) (moqSubscribeOk MoqMessageSubscribeOk, err error) {
	// rx SUBSCRIBE OK

	trackNamespace, errTrackNamespace := quichelpers.ReadString(stream, MOQ_MAX_STRING_LENGTH)
	if errTrackNamespace != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE OK reading TrackNmespace, err: %v", errTrackNamespace))
		return
	}
	moqSubscribeOk.TrackNamespace = trackNamespace

	trackName, errTrackName := quichelpers.ReadString(stream, MOQ_MAX_STRING_LENGTH)
	if errTrackName != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE OK reading trackName, err: %v", errTrackName))
		return
	}
	moqSubscribeOk.TrackName = trackName

	trackId, errTrackId := quichelpers.ReadVarint(stream)
	if errTrackId != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE OK reading TrackId, err: %v", errTrackId))
		return
	}
	moqSubscribeOk.TrackId = trackId

	expires, errExpires := quichelpers.ReadVarint(stream)
	if errExpires != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE_OK reading expires, err: %v", errExpires))
		return
	}
	moqSubscribeOk.Expires = expires

	return
}

func receiveSubscribe(stream quichelpers.IWtReadableStream) (moqSubscribe MoqMessageSubscribe, err error) {
	// rx SUBSCRIBE

	trackNamespace, errTrackNamespace := quichelpers.ReadString(stream, MOQ_MAX_STRING_LENGTH)
	if errTrackNamespace != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading TrackNmespace, err: %v", errTrackNamespace))
		return
	}
	moqSubscribe.TrackNamespace = trackNamespace

	trackName, errTrackName := quichelpers.ReadString(stream, MOQ_MAX_STRING_LENGTH)
	if errTrackName != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading trackName, err: %v", errTrackName))
		return
	}
	moqSubscribe.TrackName = trackName

	startGroupMode, errStartGroupMode := quichelpers.ReadVarint(stream)
	if errStartGroupMode != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading start group mode, err: %v", errStartGroupMode))
		return
	}
	moqSubscribe.StartGroup.Type = MoqLocationType(startGroupMode)
	if moqSubscribe.StartGroup.Type != MoqLocationTypeNone {
		startGroupValue, errStartGroupValue := quichelpers.ReadVarint(stream)
		if errStartGroupValue != nil {
			err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading start group value, err: %v", errStartGroupMode))
			return
		}
		moqSubscribe.StartGroup.Value = startGroupValue
	}

	startObjectMode, errStartObjectMode := quichelpers.ReadVarint(stream)
	if errStartObjectMode != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading start object mode, err: %v", errStartObjectMode))
		return
	}
	moqSubscribe.StartObject.Type = MoqLocationType(startObjectMode)
	if moqSubscribe.StartObject.Type != MoqLocationTypeNone {
		startObjectValue, errStarObjectValue := quichelpers.ReadVarint(stream)
		if errStarObjectValue != nil {
			err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading start object value, err: %v", errStarObjectValue))
			return
		}
		moqSubscribe.StartGroup.Value = startObjectValue
	}

	endGroupMode, errEndGroupMode := quichelpers.ReadVarint(stream)
	if errEndGroupMode != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading start group, err: %v", errEndGroupMode))
		return
	}
	moqSubscribe.EndGroup.Type = MoqLocationType(endGroupMode)
	if moqSubscribe.EndGroup.Type != MoqLocationTypeNone {
		endGroupValue, errEndGroupValue := quichelpers.ReadVarint(stream)
		if errEndGroupValue != nil {
			err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading end group value, err: %v", errEndGroupValue))
			return
		}
		moqSubscribe.EndGroup.Value = endGroupValue
	}

	endObjectMode, errEndObjectMode := quichelpers.ReadVarint(stream)
	if errEndObjectMode != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading start object, err: %v", errEndObjectMode))
		return
	}
	moqSubscribe.EndObject.Type = MoqLocationType(endObjectMode)
	if moqSubscribe.EndObject.Type != MoqLocationTypeNone {
		endObjectValue, errEndObjectValue := quichelpers.ReadVarint(stream)
		if errEndObjectValue != nil {
			err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading end object value, err: %v", errEndObjectValue))
			return
		}
		moqSubscribe.EndObject.Value = endObjectValue
	}

	params, errParams := readParameters(stream)
	if errParams != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading parameters, err: %v", errParams))
		return
	}
	foundObj, found := params[uint64(MoqParamsAuthorizationInfo)]
	if found {
		moqSubscribe.AuthInfo = foundObj.(string)
	}

	return
}

func receiveAnnounce(stream quichelpers.IWtReadableStream) (moqAnnounce MoqMessageAnnounce, err error) {
	// rx ANNOUNCE

	trackNamespace, errTrackNamespace := quichelpers.ReadString(stream, MOQ_MAX_STRING_LENGTH)
	if errTrackNamespace != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading TrackNmespace, err: %v", errTrackNamespace))
		return
	}
	moqAnnounce.TrackNamespace = trackNamespace

	params, errParams := readParameters(stream)
	if errParams != nil {
		err = errors.New(fmt.Sprintf("MOQ SUBSCRIBE reading parameters, err: %v", errParams))
		return
	}
	foundObj, found := params[uint64(MoqParamsAuthorizationInfo)]
	if found {
		moqAnnounce.AuthInfo = foundObj.(string)
	}

	return
}

func receiveSetUp(stream quichelpers.IWtReadableStream) (moqSetup MoqMessageSetup, err error) {
	// rx SETUP
	versionsLength, errVersionsLength := quichelpers.ReadVarint(stream)
	if errVersionsLength != nil {
		err = errors.New(fmt.Sprintf("MOQ SETUP reading versions length, err: %v", errVersionsLength))
		return
	}
	if versionsLength > MAX_PROTOCOL_VERSIONS {
		err = errors.New(fmt.Sprintf("MOQ SETUP max protocol versions exceeded, got %d", versionsLength))
		return
	}
	for i := 0; i < int(versionsLength); i++ {
		version, errVersion := quichelpers.ReadVarint(stream)
		if errVersion != nil {
			err = errors.New(fmt.Sprintf("MOQ SETUP reading version (%d), err: %v", i, errVersion))
			return
		}
		moqSetup.SupportedClientVersions = append(moqSetup.SupportedClientVersions, MoqVersion(version))
	}

	params, errParams := readParameters(stream)
	if errParams != nil {
		err = errors.New(fmt.Sprintf("MOQ SETUP reading parameters, err: %v", errParams))
		return
	}
	foundObj, found := params[uint64(MoqParamsRole)]
	if found {
		moqSetup.Role = MoqRole(foundObj.(uint64))
	}

	return
}

func receiveObjectHeader(stream quichelpers.IWtReadableStream) (moqObjHeader moqobject.MoqObjectHeader, err error) {
	// rx Obj header
	trackId, errTrackId := quichelpers.ReadVarint(stream)
	if errTrackId != nil {
		err = errors.New(fmt.Sprintf("MOQ OBJECT reading track id, err: %v", errTrackId))
		return
	}

	groupSeq, errGroupSeq := quichelpers.ReadVarint(stream)
	if errGroupSeq != nil {
		err = errors.New(fmt.Sprintf("MOQ OBJECT reading group sequence, err: %v", errGroupSeq))
		return
	}

	objSeq, errObjSeq := quichelpers.ReadVarint(stream)
	if errObjSeq != nil {
		err = errors.New(fmt.Sprintf("MOQ OBJECT reading object sequence, err: %v", errObjSeq))
		return
	}

	sendOrder, errSendOrder := quichelpers.ReadVarint(stream)
	if errSendOrder != nil {
		err = errors.New(fmt.Sprintf("MOQ OBJECT reading object send order, err: %v", errSendOrder))
		return
	}

	moqObjHeader.TrackId = trackId
	moqObjHeader.GroupSequence = groupSeq
	moqObjHeader.ObjectSequence = objSeq
	moqObjHeader.SendOrder = sendOrder

	return
}

func ReadObjPayloadToEOS(stream quichelpers.IWtReadableStream, moqObj *moqobject.MoqObject) error {
	// rx Obj payload

	buf := make([]byte, READ_BLOCK_SIZE_BYTES)
	var err error
	n := 0
	for {
		n, err = stream.Read(buf)
		if (err == nil || err == io.EOF) && n > 0 {
			moqObj.PayloadWrite(buf[:n])
		}
		if err != nil {
			break
		}
	}

	if err == io.EOF {
		moqObj.SetEof()
		return nil
	}
	return err
}

func SendServerSetup(stream quichelpers.IWtWritableStream, moqSetupResponse MoqMessageSetupResponse) error {

	err := quichelpers.WriteVarint(stream, uint64(MoqIdMessageServerSetup))
	if err != nil {
		return err
	}

	// Version
	err = quichelpers.WriteVarint(stream, uint64(moqSetupResponse.Version))
	if err != nil {
		return err
	}

	// Number of params
	err = quichelpers.WriteVarint(stream, uint64(1))
	if err != nil {
		return err
	}

	// Role
	err = quichelpers.WriteVarint(stream, uint64(MoqParamsRole))
	if err != nil {
		return err
	}
	// Role length
	length, errLength := quichelpers.VarIntLength(uint64(moqSetupResponse.Role))
	if errLength != nil {
		return errLength
	}
	err = quichelpers.WriteVarint(stream, uint64(length))
	if err != nil {
		return err
	}
	err = quichelpers.WriteVarint(stream, uint64(moqSetupResponse.Role))
	if err != nil {
		return err
	}
	return nil
}

func SendAnnounceOK(stream quichelpers.IWtWritableStream, moqAnnounceOk MoqMessageAnnounceOk) error {

	err := quichelpers.WriteVarint(stream, uint64(MoqIdMessageAnnounceOk))
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqAnnounceOk.TrackNamespace)
	if err != nil {
		return err
	}
	return nil
}

func SendAnnounceError(stream quichelpers.IWtWritableStream, moqAnnounceError MoqMessageAnnounceError) error {

	err := quichelpers.WriteVarint(stream, uint64(MoqIdMessageAnnounceError))
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqAnnounceError.TrackNamespace)
	if err != nil {
		return err
	}
	err = quichelpers.WriteVarint(stream, uint64(moqAnnounceError.ErrCode))
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqAnnounceError.ErrMsg)
	if err != nil {
		return err
	}
	return nil
}

func SendSubscribeOk(stream quichelpers.IWtWritableStream, moqSubscribeOk MoqMessageSubscribeOk) error {

	err := quichelpers.WriteVarint(stream, uint64(MoqIdSubscribeOk))
	if err != nil {
		return err
	}

	err = quichelpers.WriteString(stream, moqSubscribeOk.TrackNamespace)
	if err != nil {
		return err
	}

	err = quichelpers.WriteString(stream, moqSubscribeOk.TrackName)
	if err != nil {
		return err
	}

	err = quichelpers.WriteVarint(stream, moqSubscribeOk.TrackId)
	if err != nil {
		return err
	}

	err = quichelpers.WriteVarint(stream, moqSubscribeOk.Expires)
	if err != nil {
		return err
	}

	return nil
}

func SendSubscribe(stream quichelpers.IWtWritableStream, moqSubscribe MoqMessageSubscribe) error {

	err := quichelpers.WriteVarint(stream, uint64(MoqIdSubscribe))
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqSubscribe.TrackNamespace)
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqSubscribe.TrackName)
	if err != nil {
		return err
	}
	// Start group
	err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.StartGroup.Type))
	if err != nil {
		return err
	}
	if moqSubscribe.StartGroup.Type != MoqLocationTypeNone {
		err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.StartGroup.Value))
		if err != nil {
			return err
		}
	}
	// Start object
	err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.StartObject.Type))
	if err != nil {
		return err
	}
	if moqSubscribe.StartObject.Type != MoqLocationTypeNone {
		err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.StartObject.Value))
		if err != nil {
			return err
		}
	}
	// End group
	err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.EndGroup.Type))
	if err != nil {
		return err
	}
	if moqSubscribe.EndGroup.Type != MoqLocationTypeNone {
		err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.EndGroup.Value))
		if err != nil {
			return err
		}
	}
	// End object
	err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.EndObject.Type))
	if err != nil {
		return err
	}
	if moqSubscribe.EndObject.Type != MoqLocationTypeNone {
		err = quichelpers.WriteVarint(stream, uint64(moqSubscribe.EndObject.Value))
		if err != nil {
			return err
		}
	}

	// Params
	err = quichelpers.WriteVarint(stream, uint64(1))
	if err != nil {
		return err
	}
	// [0] Auth info
	err = quichelpers.WriteVarint(stream, uint64(MoqParamsAuthorizationInfo))
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqSubscribe.AuthInfo)
	if err != nil {
		return err
	}

	return nil
}

func SendSubscribeError(stream quichelpers.IWtWritableStream, moqSubscribeError MoqMessageSubscribeError) error {

	err := quichelpers.WriteVarint(stream, uint64(MoqIdSubscribeError))
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqSubscribeError.TrackNamespace)
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqSubscribeError.TrackName)
	if err != nil {
		return err
	}
	err = quichelpers.WriteVarint(stream, uint64(moqSubscribeError.ErrCode))
	if err != nil {
		return err
	}
	err = quichelpers.WriteString(stream, moqSubscribeError.ErrMsg)
	if err != nil {
		return err
	}
	return nil
}

func SendObject(stream quichelpers.IWtWritableStream, moqObj *moqobject.MoqObject) error {

	err := quichelpers.WriteVarint(stream, uint64(MoqIdMessageObject))
	if err != nil {
		return err
	}
	err = quichelpers.WriteVarint(stream, moqObj.TrackId)
	if err != nil {
		return err
	}
	err = quichelpers.WriteVarint(stream, moqObj.GroupSequence)
	if err != nil {
		return err
	}
	err = quichelpers.WriteVarint(stream, moqObj.ObjectSequence)
	if err != nil {
		return err
	}
	err = quichelpers.WriteVarint(stream, moqObj.SendOrder)
	if err != nil {
		return err
	}

	dataBlock := make([]byte, READ_BLOCK_SIZE_BYTES)
	srcReader := moqObj.NewReader()
	readBytes := 0
	totalSent := 0
	var errRead error = nil
	for errRead == nil {
		readBytes, errRead = srcReader.Read(dataBlock)
		if readBytes > 0 {
			stream.Write(dataBlock[:readBytes])
			totalSent += readBytes
		}
	}
	return nil
}

// Helpers

func readParameters(stream quichelpers.IWtReadableStream) (parameters map[uint64]any, err error) {
	parameters = map[uint64]any{}
	numParamsLength, errNumParamsLength := quichelpers.ReadVarint(stream)
	if errNumParamsLength != nil {
		err = errors.New(fmt.Sprintf("MOQ parameters reading number of params, err: %v", errNumParamsLength))
		return
	}
	if numParamsLength > MAX_PARAMS {
		err = errors.New(fmt.Sprintf("MOQ parameters exceeded max number of params %d, received: %d", MAX_PARAMS, numParamsLength))
		return
	}
	for i := 0; i < int(numParamsLength); i++ {
		paramId, errParamId := quichelpers.ReadVarint(stream)
		if errParamId != nil {
			err = errors.New(fmt.Sprintf("MOQ parameters reading paramId in position %d, err: %v", i, errNumParamsLength))
			return
		}
		if MoqParams(paramId) == MoqParamsAuthorizationInfo {
			authInfo, errAuthInfo := quichelpers.ReadString(stream, MOQ_MAX_STRING_LENGTH)
			if errAuthInfo != nil {
				err = errors.New(fmt.Sprintf("MOQ parameters reading auth info, err: %v", errAuthInfo))
				return
			}
			parameters[paramId] = authInfo

		} else if MoqParams(paramId) == MoqParamsRole {
			_, errLength := quichelpers.ReadVarint(stream)
			if errLength != nil {
				err = errors.New(fmt.Sprintf("MOQ parameters role reading param length info, err: %v", errLength))
				return
			}
			role, errRole := quichelpers.ReadVarint(stream)
			if errRole != nil {
				err = errors.New(fmt.Sprintf("MOQ parameters reading role, err: %v", errRole))
				return
			}
			parameters[paramId] = role

		} else {
			length, errLength := quichelpers.ReadVarint(stream)
			if errLength != nil {
				err = errors.New(fmt.Sprintf("MOQ parameters reading param length info, err: %v", errLength))
				return
			}
			tmpBuffer := make([]byte, length)
			errReadingUnknown := quichelpers.ReadBytes(stream, tmpBuffer)
			if errReadingUnknown != nil {
				err = errors.New(fmt.Sprintf("MOQ parameters reading unknown param blob, err: %v", errLength))
				return
			}
		}
	}
	return
}
