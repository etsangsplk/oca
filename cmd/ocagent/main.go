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

package main

import (
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/census-instrumentation/oca"
	"github.com/census-instrumentation/oca/rpc"
)

func main() {
	ln, err := net.Listen("tcp", ":9675")
	if err != nil {
		log.Fatalf("Failed to register net.Listener: %v", err)
	}
	defer ln.Close()

	srv := grpc.NewServer()
	agent, err := oca.NewAgent()
	if err != nil {
		log.Fatalf("Failed to create the agent: %v", err)
	}
	rpc.RegisterTraceServiceServer(srv, agent)

	errsChan := make(chan error, 1)
	go func() {
		errsChan <- srv.Serve(ln)
	}()
	log.Printf("Serving at %s\n", ln.Addr())
	if err := <-errsChan; err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
