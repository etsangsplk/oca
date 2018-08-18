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

package ocaclient_test

import (
	"context"
        "net"
	"testing"
	"time"

	"go.opencensus.io/trace"
	"google.golang.org/grpc"

	"github.com/census-instrumentation/oca"
	"github.com/census-instrumentation/oca/ocaclient"
	"github.com/census-instrumentation/oca/rpc"
)

func TestEndToEndPushes(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to create listener server: %v", err)
	}
	defer ln.Close()

	srv := grpc.NewServer()
	agent, err := oca.NewAgent()
	if err != nil {
		t.Fatalf("Failed to create the agent: %v", err)
	}
	rpc.RegisterTraceServiceServer(srv, agent)

	go func() {
		_ = srv.Serve(ln)
	}()

	cn, err := ocaclient.New(ln.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create ocaclient.Client: %v", err)
	}
	defer cn.Close()

	trace.RegisterExporter(cn)
	trace.ApplyConfig(cn.TraceConfig())
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	for i := 0; i < 100; i++ {
		_, span := trace.StartSpan(context.Background(), "/Introduction")
		<-time.After(6 * time.Millisecond)
		span.End()
	}

	<-time.After(15 * time.Second)
}
