// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oca

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/census-instrumentation/oca/rpc"
	opencensus_proto_trace "github.com/census-instrumentation/opencensus-proto/gen-go/traceproto"
)

type agent struct {
	defaultTraceConfigMu      sync.Mutex
	defaultInitialTraceConfig *rpc.ConfigRequest

	perClientConfigStorage KeyValueStore
	configRequestChannels  KeyValueStore
}

var _ rpc.TraceServiceServer = (*agent)(nil)

func (a *agent) PushConfig(srv rpc.TraceService_PushConfigServer) error {
	pushCount := uint64(0)

	// Firstly push to them the initial configuration
	if err := srv.Send(a.defaultInitialTraceConfig); err != nil {
		// TODO: Log and report this error
		return fmt.Errorf("PushConfig: sending first message: %v", err)
	}

	// The first message should contain the identifier
	firstConfigResponse, err := srv.Recv()
	if err != nil {
		return fmt.Errorf("PushConfig: receiving first message and identifier from client: %v", err)
	}

	clientIdentifier, err := retrieveClientIdentifier(firstConfigResponse.Identifier)
	if err != nil {
		return fmt.Errorf("PushConfig: retrieving client identifier: %v", err)
	}

	// clientCentricConfig contains the object on which configuration
	// updates for the specific client will be sent back on, but also
	// to store any related state for the client.
	clientCentricConfig, err := a.subscribeToClientConfigChanges(clientIdentifier)
	if err != nil {
		return fmt.Errorf("PushConfig: subscribing to client config changes: %v", err)
	}

	for {
		updateCfg := <-clientCentricConfig.C

		// We've received it from the updating side,
		// now push it down to the client
		if err := srv.Send(updateCfg); err != nil {
			// TODO: Perhaps this condition shouldn't cause us to abort?
			return fmt.Errorf("PushConfig: Send updateConfig: %v", err)
		}

		// TODO: maintain persistent stats of successful
		// config pushes and when they were performed
		pushCount += 1
	}

	return nil
}

// ExportTraces is a client-centric service that the agent hinges onto
// to receive traces that are exported by each downstream client.
func (a *agent) ExportTraces(srv rpc.TraceService_ExportTracesServer) error {
	// Firstly wait for them to send the initial trace with an identifier
	firstSpanExport, err := srv.Recv()
	if err != nil {
		// TODO: Log and report this error
		return fmt.Errorf("ExportTraces: sending first message: %v", err)
	}

	clientIdentifier, err := retrieveClientIdentifier(firstSpanExport.Identifier)
	if err != nil {
		return fmt.Errorf("ExportTraces: retrieving client identifier: %v", err)
	}

	sProcessor, ok, err := a.retrievePerClientSpansProcessor(clientIdentifier)
	if err != nil {
		return fmt.Errorf("ExportTraces: retrieving per-client-trace processor: %v", err)
	}
	if !ok || sProcessor == nil {
		// TODO: In this case what do we do if sProcessor is not set?
		// Currently using the noopLoggingSpansProcessor but perhaps we should
		// indicate to the client that their spans won't be exported until they've
		// configured an exporting backend?
		sProcessor = new(noopLoggingSpansProcessor)
	}

	// Process the incoming spans now
	for {
		recv, err := srv.Recv()
		if err != nil {
			// TODO: add retry mechanism here
			return err
		}

                fmt.Printf("recv: %v\n", recv)
		if len(recv.Spans) > 0 {
			if err := sProcessor.ProcessSpans(recv.Spans...); err != nil {
				// TODO: Examine failed trace processing
			}

			// TODO: maintain persistent stats of successful span
			// processing counts, when and how they were performed.
		}
	}

	return nil
}

var errNilConfigIdentifier = errors.New("expecting a non-nil client identifier")

func retrieveClientIdentifier(identifier *rpc.Identifier) (string, error) {
	if identifier == nil {
		return "", errNilConfigIdentifier
	}
	// Now create the unique identifier key per client
	key := fmt.Sprintf("%d-%d-%s", identifier.Pid, identifier.StartTime, identifier.Hostname)
	return key, nil
}

func (a *agent) subscribeToClientConfigChanges(clientIdentifier string) (*clientCentricConfig, error) {
	// Firstly check if we've got this client's configuration already
	saved, ok, err := a.perClientConfigStorage.Get(clientIdentifier)
	if err != nil {
		return nil, err
	}
	if ok && saved != nil {
		cCfg := saved.(*clientCentricConfig)
		if _, err := a.ensureSubscribed(clientIdentifier); err != nil {
			return nil, err
		}
		return cCfg, nil
	}

	// Otherwise now create the first clientCentricConfig
	// on which we'll receive client configuration changes,
	// such as if it is changed from the agent's UI.
	cCfg, err := a.newClientCentricConfig(clientIdentifier)
	if err != nil {
		return nil, err
	}
	// Now finally store it
	if err := a.perClientConfigStorage.Put(clientIdentifier, cCfg); err != nil {
		return nil, err
	}
	return cCfg, nil
}

type clientCentricConfig struct {
	C <-chan *rpc.ConfigRequest
}

func (a *agent) newClientCentricConfig(key string) (*clientCentricConfig, error) {
	notifChan, err := a.ensureSubscribed(key)
	if err != nil {
		return nil, err
	}
	return &clientCentricConfig{C: notifChan}, nil
}

func (a *agent) ensureSubscribed(clientIdentifier string) (chan *rpc.ConfigRequest, error) {
	got, ok, err := a.configRequestChannels.Get(clientIdentifier)
	if err != nil {
		return nil, err
	}
	if ok && got != nil {
		notifChan, _ := got.(chan *rpc.ConfigRequest)
		if notifChan != nil {
			// Successfully got an already set/saved notifications channel
			return notifChan, nil
		}
	}

	// Otherwise time to create that notifications channel.
	// TODO: Figure out if we should fan out to multiple channels sharing the same identifier?
	// For starters and for simplicity, only one channel will the connected per key identifier
	notifChan := make(chan *rpc.ConfigRequest)
	go func() {
		// Prepare the initial trace configuration
		notifChan <- a.defaultInitialTraceConfig
	}()
	// Now store the created notification channel
	if err := a.configRequestChannels.Put(clientIdentifier, notifChan); err != nil {
		return nil, err
	}
	return notifChan, nil
}

func NewAgent() (*agent, error) {
	a := &agent{
		perClientConfigStorage: NewInMemoryKeyValueStore(),
		configRequestChannels:  NewInMemoryKeyValueStore(),

		defaultInitialTraceConfig: &rpc.ConfigRequest{
			SamplingRate: 1 / 10.0,
		},
	}
	return a, nil
}

type timeEvent struct {
	StartTime time.Time
	EndTime   time.Time
	Count     uint64
}

type SpansProcessor interface {
	ProcessSpans(sd ...*opencensus_proto_trace.Span) error
}

func (a *agent) retrievePerClientSpansProcessor(clientIdentifier string) (SpansProcessor, bool, error) {
	return nil, false, nil
}

type noopLoggingSpansProcessor int

var _ SpansProcessor = (*noopLoggingSpansProcessor)(nil)

func (nlsp *noopLoggingSpansProcessor) ProcessSpans(sd ...*opencensus_proto_trace.Span) error {
	log.Printf("[NoopLoggingSpansProcessor] dropping %d spans:: %+v\n", len(sd), sd)
        return nil
}
