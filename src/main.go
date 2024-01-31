/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package main

import (
	"context"
	"encoding/json"
	"errors"
	"facebookexperimental/moq-go-server/moqconnectionmanagment"
	"facebookexperimental/moq-go-server/moqfwdtable"
	"facebookexperimental/moq-go-server/moqmessageobjects"
	"facebookexperimental/moq-go-server/moqorigins"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	log "github.com/sirupsen/logrus"
)

// Default parameters
const HTTP_SERVER_LISTEN_ADDR = ":4433"
const TLS_CERT_FILEPATH = "../certs/certificate.pem"
const TLS_KEY_FILEPATH = "../certs/certificate.key"
const OBJECT_EXPIRATION_MS = 3 * 60 * 1000
const CACHE_CLEAN_UP_PERIOD_MS = 10 * 1000
const HTTP_CONNECTION_KEEP_ALIVE_MS = 10 * 1000
const MOQ_ORIGINS_FILEPATH = "../origins/origins.json"

// Main function

func main() {
	// Parse params
	listenAddr := flag.String("listen_addr", HTTP_SERVER_LISTEN_ADDR, "Server listen port (example: \":4433\")")
	tlsCertPath := flag.String("tls_cert", TLS_CERT_FILEPATH, "TLS certificate file path to use in this server")
	tlsKeyPath := flag.String("tls_key", TLS_KEY_FILEPATH, "TLS key file path to use in this server")
	objExpMs := flag.Uint64("obj_exp_ms", OBJECT_EXPIRATION_MS, "Object TTL in this server (in milliseconds)")
	cacheCleanUpPeriodMs := flag.Uint64("cache_cleanup_period_ms", CACHE_CLEAN_UP_PERIOD_MS, "Execute clean up task every (in milliseconds)")
	httpConnTimeoutMs := flag.Uint64("http_conn_time_out_ms", HTTP_CONNECTION_KEEP_ALIVE_MS, "HTTP connection timeout (in milliseconds)")
	moqOriginsConfigFile := flag.String("moq_origins_config", MOQ_ORIGINS_FILEPATH, "Json file with list of MOQ content origins")

	flag.Parse()

	log.SetFormatter(&log.TextFormatter{})

	ctx, cancel := context.WithCancel(context.Background())

	// Create moqt obj forward table
	moqtFwdTable := moqfwdtable.New()

	// create objects mem storage (relay)
	objects := moqmessageobjects.New(*cacheCleanUpPeriodMs)

	// Load and create origins
	moqOrigins, errOrigins := loadAndInitializeMoqOrigins(*moqOriginsConfigFile)
	if errOrigins != nil {
		log.Error(fmt.Sprintf("Can not load/parse origins data from file %s. Err: %s", *moqOriginsConfigFile, errOrigins))
	} else {
		log.Info(fmt.Sprintf("Loaded origins: %s", moqOrigins.ToString()))
	}

	s := webtransport.Server{
		CheckOrigin: CheckCORSOrigin,
		H3: http3.Server{Addr: *listenAddr,
			QuicConfig: &quic.Config{
				KeepAlivePeriod: time.Duration(*httpConnTimeoutMs/1000) * time.Second,
				MaxIdleTimeout:  time.Duration(3*(*httpConnTimeoutMs/1000)) * time.Second,
			}}}

	// Catch ctrl+C
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		// Exit server
		log.Info("Intercepted KILL SIGTERM")
		cancel()
		s.Close()
	}()

	http.HandleFunc("/moq", func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.Upgrade(w, r)
		if err != nil {
			log.Error(fmt.Sprintf("Upgrading failed. Err: %v", err))
			w.WriteHeader(500)
			return
		}

		namespace := r.URL.Path
		log.Info(fmt.Sprintf("%s - Accepted incoming WebTransport session. rawQuery: %s", namespace, r.URL.RawQuery))

		moqconnectionmanagment.MoqConnectionManagment(ctx, conn, namespace, moqtFwdTable, objects, *objExpMs)
	})

	log.Info(fmt.Sprintf("Serving WT. Addr: %s, Cert file: %s, Key file: %s", *listenAddr, *tlsCertPath, *tlsKeyPath))
	errSvr := s.ListenAndServeTLS(*tlsCertPath, *tlsKeyPath)
	if errSvr != nil {
		log.Error(fmt.Sprintf("Error starting server. Err: %v", errSvr))
	}

	objects.Stop()
	moqOrigins.Close()
}

// CORS helper

func CheckCORSOrigin(r *http.Request) bool {
	// TODO: Allow only specific CORS origins
	return true
}

// Origins helper

func loadAndInitializeMoqOrigins(originsFilepath string) (moqOrigins *moqorigins.MoqOrigins, err error) {
	moqOrigins = moqorigins.New()
	if originsFilepath != "" {
		// read file
		originsJsonData, errOriginLoad := os.ReadFile(originsFilepath)
		if errOriginLoad != nil {
			err = errOriginLoad
			return
		}
		// Parse file
		var originsData moqorigins.MoqOriginsData
		errOriginParse := json.Unmarshal(originsJsonData, &originsData)
		if errOriginParse != nil {
			err = errOriginParse
			return
		}

		// Load certificates (if needed)
		for i := range originsData.MoqOrigins {
			if originsData.MoqOrigins[i].OriginCertPath != "" {
				filePath := filepath.Join(filepath.Dir(originsFilepath), originsData.MoqOrigins[i].OriginCertPath)
				data, errLoadCert := os.ReadFile(filePath)
				if errLoadCert != nil {
					err = errors.New(fmt.Sprintf("We could NOT load cert file %s. Err: %v", filePath, errLoadCert))
					return
				}
				originsData.MoqOrigins[i].CertData = data
			}
		}

		// Create origins
		moqOrigins.Initialize(originsData)
	}

	return moqOrigins, err
}
