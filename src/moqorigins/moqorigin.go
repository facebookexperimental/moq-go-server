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
	"facebookexperimental/moq-go-server/moqconnectionmanagment"
	"facebookexperimental/moq-go-server/moqfwdtable"
	"facebookexperimental/moq-go-server/moqmessageobjects"
	"fmt"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	log "github.com/sirupsen/logrus"
)

const RECONNECT_DELAY_MS = 3000

type MoqOriginData struct {
	FriendlyName   string `json:"friendlyname"`
	Guid           string `json:"guid"`
	TrackNamespace string `json:"tracknamespace"`
	AuthInfo       string `json:"authinfo"`
	OriginAddress  string `json:"originaddress"`
	OriginCertPath string `json:"origincertpath"`
	CertData       []byte
}

type MoqOrigin struct {
	moqOriginData MoqOriginData

	// Housekeeping thread channel
	cleanUpChannel chan bool

	// Used for WT
	d            *webtransport.Dialer
	roundTripper *http3.RoundTripper
}

type moqOriginExt struct {
	MoqOriginData
	moqOriginPtr *MoqOrigin
}

// New Creates a new moq origin
func newOrigin(moqOriginData MoqOriginData, moqtFwdTable *moqfwdtable.MoqFwdTable, objects *moqmessageobjects.MoqMessageObjects, objExpMs uint64) *MoqOrigin {
	mor := MoqOrigin{moqOriginData, make(chan bool), nil, nil}

	// Start process thread
	go mor.process(mor.cleanUpChannel, moqtFwdTable, objects, objExpMs)

	return &mor
}

func (mor *MoqOrigin) Close() (err error) {
	// Send finish signal
	mor.cleanUpChannel <- true

	// Wait to finish
	<-mor.cleanUpChannel

	if mor.d != nil {
		mor.d.Close()
	}
	if mor.roundTripper != nil {
		mor.roundTripper.Close()
	}
	mor.d = nil
	mor.roundTripper = nil

	return
}

func (mor *MoqOrigin) process(cleanUpChannelBidi chan bool, moqtFwdTable *moqfwdtable.MoqFwdTable, objects *moqmessageobjects.MoqMessageObjects, objExpMs uint64) {
	log.Info(fmt.Sprintf("%s Entering origin process thread", mor.moqOriginData.FriendlyName))

	ctx, cancel := context.WithCancel(context.Background())

	// TODO: Reconnect if disconnected

	go mor.processClientSession(ctx, moqtFwdTable, objects, objExpMs)

	select {
	case <-cleanUpChannelBidi:
		log.Info(fmt.Sprintf("%s Received exit signal", mor.moqOriginData.FriendlyName))
	}
	cancel()

	// Indicates finished
	cleanUpChannelBidi <- true

	log.Info(fmt.Sprintf("%s Exited origin process thread", mor.moqOriginData.FriendlyName))
}

func (mor *MoqOrigin) processClientSession(ctx context.Context, moqtFwdTable *moqfwdtable.MoqFwdTable, objects *moqmessageobjects.MoqMessageObjects, objExpMs uint64) {

	// Loop until context cancelled
	for ctx.Err() == nil {
		session, errConn := mor.connectClientWT(ctx, mor.moqOriginData.OriginAddress, mor.moqOriginData.CertData)
		if errConn != nil {
			log.Error(fmt.Sprintf("%s - error connecting WT to: %s. Err %v", mor.moqOriginData.FriendlyName, mor.moqOriginData.OriginAddress, errConn))
		} else {
			log.Info(fmt.Sprintf("%s - Connected WT", mor.moqOriginData.FriendlyName))

			moqconnectionmanagment.MoqConnectionManagment(true, mor.moqOriginData.TrackNamespace, mor.moqOriginData.AuthInfo, ctx, session, mor.moqOriginData.FriendlyName, moqtFwdTable, objects, objExpMs)
		}
		sleepWithContext(ctx, RECONNECT_DELAY_MS*time.Millisecond)
	}
	return
}

func (mor *MoqOrigin) connectClientWT(ctx context.Context, addr string, cert []byte) (session *webtransport.Session, err error) {

	var d webtransport.Dialer
	mor.d = &d
	if cert != nil {
		pool, errPool := x509.SystemCertPool()
		if errPool != nil {
			log.Error(fmt.Sprintf("%s - Loading local cert pool. Err: %v", mor.moqOriginData.FriendlyName, errPool))
			return
		}
		pool.AppendCertsFromPEM(cert)

		mor.roundTripper = &http3.RoundTripper{
			TLSClientConfig: &tls.Config{
				RootCAs:            pool,
				InsecureSkipVerify: false,
			},
		}
		mor.d.RoundTripper = mor.roundTripper
	}
	_, session, err = mor.d.Dial(ctx, addr, nil)

	return
}

// Helpers

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return fmt.Errorf("Interrupted")
	case <-t.C:
	}
	return nil
}
