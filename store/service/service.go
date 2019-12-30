// Package service implements the store service interface
package service

import (
	"context"
	"io"
	"time"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/store"
	pb "github.com/micro/go-micro/store/service/proto"
)

type serviceStore struct {
	options store.Options

	// Namespace to use
	Namespace string

	// Addresses of the nodes
	Nodes []string

	// Prefix to use
	Prefix string

	// store service client
	Client pb.StoreService
}

func (s *serviceStore) Context() context.Context {
	ctx := context.Background()

	md := make(metadata.Metadata)

	if len(s.Namespace) > 0 {
		md["Micro-Namespace"] = s.Namespace
	}

	if len(s.Prefix) > 0 {
		md["Micro-Prefix"] = s.Prefix
	}

	return metadata.NewContext(ctx, md)
}

// Sync all the known records
func (s *serviceStore) List() ([]*store.Record, error) {
	stream, err := s.Client.List(s.Context(), &pb.ListRequest{}, client.WithAddress(s.Nodes...))
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	var records []*store.Record

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		for _, record := range rsp.Records {
			records = append(records, &store.Record{
				Key:    record.Key,
				Value:  record.Value,
				Expiry: time.Duration(record.Expiry) * time.Second,
			})
		}
	}

	return records, nil
}

// Read a record with key
func (s *serviceStore) Read(keys ...string) ([]*store.Record, error) {
	rsp, err := s.Client.Read(s.Context(), &pb.ReadRequest{
		Keys: keys,
	}, client.WithAddress(s.Nodes...))
	if err != nil {
		return nil, err
	}

	records := make([]*store.Record, 0, len(rsp.Records))
	for _, val := range rsp.Records {
		records = append(records, &store.Record{
			Key:    val.Key,
			Value:  val.Value,
			Expiry: time.Duration(val.Expiry) * time.Second,
		})
	}
	return records, nil
}

// Write a record
func (s *serviceStore) Write(recs ...*store.Record) error {
	records := make([]*pb.Record, 0, len(recs))

	for _, record := range recs {
		records = append(records, &pb.Record{
			Key:    record.Key,
			Value:  record.Value,
			Expiry: int64(record.Expiry.Seconds()),
		})
	}

	_, err := s.Client.Write(s.Context(), &pb.WriteRequest{
		Records: records,
	}, client.WithAddress(s.Nodes...))

	return err
}

// Delete a record with key
func (s *serviceStore) Delete(keys ...string) error {
	_, err := s.Client.Delete(s.Context(), &pb.DeleteRequest{
		Keys: keys,
	}, client.WithAddress(s.Nodes...))
	return err
}

// NewStore returns a new store service implementation
func NewStore(opts ...store.Option) store.Store {
	var options store.Options
	for _, o := range opts {
		o(&options)
	}

	service := &serviceStore{
		options:   options,
		Namespace: options.Namespace,
		Prefix:    options.Prefix,
		Nodes:     options.Nodes,
		Client:    pb.NewStoreService("go.micro.store", client.DefaultClient),
	}

	return service
}
