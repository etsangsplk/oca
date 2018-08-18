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

package ocaclient

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	timestamp "github.com/golang/protobuf/ptypes/timestamp"

	"github.com/census-instrumentation/oca/rpc"
	opencensus_proto_trace "github.com/census-instrumentation/opencensus-proto/gen-go/traceproto"
	"go.opencensus.io/trace"
)

type Client struct {
	traceServiceClient rpc.TraceServiceClient

	pushConfigClient  rpc.TraceService_PushConfigClient
	traceExportClient rpc.TraceService_ExportTracesClient

	closeConnOnce sync.Once
	conn          *grpc.ClientConn

	spanDataMu sync.Mutex
	spanData   []*trace.SpanData
	cancelChan chan bool
}

func New(serverAddress string) (*Client, error) {
	var dialOpts []grpc.DialOption
	if !strings.HasPrefix(serverAddress, "https://") {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}
	cc, err := grpc.Dial(serverAddress, dialOpts...)
	if err != nil {
		return nil, err
	}

	tsc := rpc.NewTraceServiceClient(cc)
	traceExportClient, err := tsc.ExportTraces(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Failed to create ExportTraces client: %v", err)
	}

	pushConfigClient, err := tsc.PushConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Failed to create PushConfig client: %v", err)
	}

	client := &Client{
		traceServiceClient: tsc, conn: cc,
		traceExportClient: traceExportClient,
		pushConfigClient:  pushConfigClient,

		// TODO: This is an arbitrary channel capacity that's meant to
		// allow 2,500 write/second by default given that the default
		// reporting period is 4 seconds. Figure out an ideal initial capacity.
		spanData: make([]*trace.SpanData, 0, 10000),
	}

	if err := client.sendIdentifierToAgent(context.Background()); err != nil {
		return nil, err
	}

	cancelChan := make(chan bool)
	// In the background start the backgroundBatchingExport goroutine
	go func() {
		client.backgroundBatchingExport(cancelChan)
	}()

	client.cancelChan = cancelChan

	return client, nil
}

func (c *Client) sendIdentifierToAgent(ctx context.Context) error {
	// Time to push them all
	hostname, _ := os.Hostname()
	firstTrace := &rpc.Traces{
		Identifier: &rpc.Identifier{
			Pid:       int64(os.Getpid()),
			Hostname:  hostname,
			StartTime: float64(time.Now().Unix()),
			Attributes: map[string]string{
				"language": "go",
			},
		},
	}
	return c.traceExportClient.Send(firstTrace)
}

func (c *Client) Close() error {
	c.closeConnOnce.Do(func() {
		c.conn.Close()
		close(c.cancelChan)
	})
	return nil
}

// This client is always an OpenCensus-Trace exporter
var _ trace.Exporter = (*Client)(nil)

func (c *Client) ExportSpan(sd *trace.SpanData) {
	c.spanData = append(c.spanData, sd)
}

func (c *Client) backgroundBatchingExport(cancel chan bool) error {
	reportingPeriod := 4 * time.Second

	for {
		select {

		case <-time.After(reportingPeriod):
			fmt.Printf("AFter: %s\n", reportingPeriod)
			c.spanDataMu.Lock()
			if len(c.spanData) > 0 {
				// Time to push them all
				traces := &rpc.Traces{
					Spans: transformOCSpansToProtoSpans(c.spanData),
				}
				c.traceExportClient.Send(traces)
				fmt.Printf("c.spanData: %#v\n", c.spanData)
				// Now reset the spandata list
				c.spanData = c.spanData[:0]
			}
			c.spanDataMu.Unlock()

		case <-cancel:
			return nil
		}
	}

	return nil
}

func transformOCSpansToProtoSpans(sdl []*trace.SpanData) []*opencensus_proto_trace.Span {
	if len(sdl) == 0 {
		return nil
	}
	ptl := make([]*opencensus_proto_trace.Span, 0, len(sdl))
	for _, sd := range sdl {
		ptl = append(ptl, toProtoSpan(sd))
	}

	return ptl
}

func toTimestamp(t time.Time) *timestamp.Timestamp {
	return &timestamp.Timestamp{
		Seconds: t.Unix(),
	}
}

func toProtoSpan(sd *trace.SpanData) *opencensus_proto_trace.Span {
	return &opencensus_proto_trace.Span{
		TraceId:      sd.TraceID[:],
		SpanId:       sd.SpanID[:],
		ParentSpanId: sd.ParentSpanID[:],
		Name:         &opencensus_proto_trace.TruncatableString{Value: sd.Name},
		Status: &opencensus_proto_trace.Status{
			Code:    sd.Code,
			Message: sd.Message,
		},
		// Kind:      opencensus_proto_trace.Span_SpanKind(sd.Kind),
		StartTime: toTimestamp(sd.StartTime),
		EndTime:   toTimestamp(sd.EndTime),
	}
}

func (c *Client) TraceConfig() trace.Config {
	// Get the last received config
	var tc trace.Config
	recv, err := c.pushConfigClient.Recv()
	if err == nil {
            fmt.Printf("Received sampling rate: %#v\n", recv)
		tc.DefaultSampler = trace.ProbabilitySampler(recv.SamplingRate)
	} else {
		tc.DefaultSampler = trace.AlwaysSample()
	}
	return tc
}
