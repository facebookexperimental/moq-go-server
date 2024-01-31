/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqorigins

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	log "github.com/sirupsen/logrus"
)

type MoqOriginData struct {
	FriendlyName   string `json:"friendlyname"`
	Guid           string `json:"guid"`
	UrlRegExpStr   string `json:"urlregexp"`
	OriginAddress  string `json:"originaddress"`
	OriginCertPath string `json:"origincertpath"`
	CertData       []byte
}

type MoqOrigin struct {
	moqOriginData MoqOriginData

	// Housekeeping thread channel
	cleanUpChannel chan bool
}

type moqOriginExt struct {
	MoqOriginData
	moqOriginPtr *MoqOrigin
}

// New Creates a new moq origin
func newOrigin(moqOriginData MoqOriginData) *MoqOrigin {
	mor := MoqOrigin{moqOriginData, make(chan bool)}

	// Start process thread
	go mor.process(mor.cleanUpChannel)

	return &mor
}

func (mor *MoqOrigin) Close() (err error) {
	// Send finish signal
	mor.cleanUpChannel <- true

	// Wait to finish
	<-mor.cleanUpChannel

	return
}

func (mor *MoqOrigin) process(cleanUpChannelBidi chan bool) {
	exit := false

	log.Info(fmt.Sprintf("%s Entering origin process thread", mor.moqOriginData.FriendlyName))

	ctx, cancel := context.WithCancel(context.Background())

	go mor.processClientSession(ctx)

	for !exit {
		select {
		case <-cleanUpChannelBidi:
			exit = true
		}
		// TODO more

		// err is only nil if rsp.StatusCode is a 2xx
		// Handle the connection. Here goes the application logic.
	}
	cancel()

	// Indicates finished
	cleanUpChannelBidi <- true

	log.Info(fmt.Sprintf("%s Exited origin process thread", mor.moqOriginData.FriendlyName))
}

func (mor *MoqOrigin) processClientSession(ctx context.Context) {
	session, errConn := mor.connectClientWT(ctx, mor.moqOriginData.OriginAddress, mor.moqOriginData.CertData)
	if errConn != nil {
		log.Error(fmt.Sprintf("%s - error connecting WT to: %s. Err %v", mor.moqOriginData.FriendlyName, mor.moqOriginData.OriginAddress, errConn))
		return
	}
	log.Info(fmt.Sprintf("%s - Connected WT", mor.moqOriginData.FriendlyName))

	controlStreamPtr, errStream := mor.moqCreateControlStream(session)
	if errStream != nil {
		log.Error(fmt.Sprintf("%s - Creating bidirectional CONTROL stream. Err: %v", mor.moqOriginData.FriendlyName, errStream))
		return
	}
	log.Error(fmt.Sprintf("%s - Created bidirectional CONTROL stream", mor.moqOriginData.FriendlyName))

	mor.moqSendSetup(controlStreamPtr)
}

func (mor *MoqOrigin) connectClientWT(ctx context.Context, addr string, cert []byte) (session *webtransport.Session, err error) {

	var d webtransport.Dialer
	if cert != nil {
		pool, errPool := x509.SystemCertPool()
		if errPool != nil {
			log.Error(fmt.Sprintf("%s - Loading local cert pool. Err: %v", mor.moqOriginData.FriendlyName, errPool))
			return
		}
		pool.AppendCertsFromPEM(cert)

		roundTripper := &http3.RoundTripper{
			TLSClientConfig: &tls.Config{
				RootCAs:            pool,
				InsecureSkipVerify: false,
			},
		}
		d.RoundTripper = roundTripper
		//defer roundTripper.Close()
	}
	_, session, err = d.Dial(ctx, addr, nil)

	return
}

func (mor *MoqOrigin) moqCreateControlStream(session *webtransport.Session) (controlStreamPtr *webtransport.Stream, err error) {
	controlStream, errOpen := session.OpenStream()
	err = errOpen
	if errOpen == nil {
		controlStreamPtr = &controlStream
	}
	return
}

func (mor *MoqOrigin) moqSendSetup(ontrolStreamPtr *webtransport.Stream) (err error) {
	err = nil
	//TODO: fill this out
	// How to mix client connections  from QUIC-GO with server from adriancable????
	// AND allow local tetsing, I can set up remote debugging!
	return
}
