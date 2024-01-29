/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package main

import (
	"context"
	"facebookexperimental/moq-go-server/moqconnectionmanagment"
	"facebookexperimental/moq-go-server/moqfwdtable"
	"facebookexperimental/moq-go-server/moqmessageobjects"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/adriancable/webtransport-go"

	log "github.com/sirupsen/logrus"
)

// Default parameters
const HTTP_SERVER_LISTEN_ADDR = ":4433"
const TLS_CERT_FILEPATH = "../certs/certificate.pem"
const TLS_KEY_FILEPATH = "../certs/certificate.key"
const OBJECT_EXPIRATION_MS = 3 * 60 * 1000
const CACHE_CLEAN_UP_PERIOD_MS = 10 * 1000
const HTTP_CONNECTION_KEEP_ALIVE_MS = 10 * 1000

// Main function

func main() {
	// Parse params
	listenAddr := flag.String("listen_addr", HTTP_SERVER_LISTEN_ADDR, "Server listen port (example: \":4433\")")
	tlsCertPath := flag.String("tls_cert", TLS_CERT_FILEPATH, "TLS certificate file path to use in this server")
	tlsKeyPath := flag.String("tls_key", TLS_KEY_FILEPATH, "TLS key file path to use in this server")
	objExpMs := flag.Uint64("obj_exp_ms", OBJECT_EXPIRATION_MS, "Object TTL in this server (in milliseconds)")
	cacheCleanUpPeriodMs := flag.Uint64("cache_cleanup_period_ms", CACHE_CLEAN_UP_PERIOD_MS, "Execute clean up task every (in milliseconds)")
	httpConnTimeoutMs := flag.Uint64("http_conn_time_out_ms", HTTP_CONNECTION_KEEP_ALIVE_MS, "HTTP connection timeout (in milliseconds)")

	flag.Parse()

	log.SetFormatter(&log.TextFormatter{})

	// Create moqt obj forward table
	moqtFwdTable := moqfwdtable.New()

	// create objects mem storage (relay)
	objects := moqmessageobjects.New(*cacheCleanUpPeriodMs)

	http.HandleFunc("/moq", func(rw http.ResponseWriter, r *http.Request) {
		session := r.Body.(*webtransport.Session)
		session.AcceptSession()
		// session.RejectSession(400)

		namespace := r.URL.Path
		log.Info(fmt.Sprintf("%s - Accepted incoming WebTransport session. rawQuery: %s", namespace, r.URL.RawQuery))

		moqconnectionmanagment.MoqConnectionManagment(session, namespace, moqtFwdTable, objects, *objExpMs)
	})

	server := &webtransport.Server{
		ListenAddr: *listenAddr,
		TLSCert:    webtransport.CertFile{Path: *tlsCertPath},
		TLSKey:     webtransport.CertFile{Path: *tlsKeyPath},
		QuicConfig: &webtransport.QuicConfig{
			KeepAlivePeriod: time.Duration(*httpConnTimeoutMs/1000) * time.Second,
			MaxIdleTimeout:  time.Duration(3*(*httpConnTimeoutMs/1000)) * time.Second,
		},
	}

	log.Info("Launching WebTransport server at: ", server.ListenAddr)
	ctx, cancel := context.WithCancel(context.Background())
	if err := server.Run(ctx); err != nil {
		log.Error(fmt.Sprintf("Server error: %s", err))
		cancel()
	}

	objects.Stop()
}
