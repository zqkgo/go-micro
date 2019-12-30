package kubernetes

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/micro/go-micro/debug/log"
)

func write(l log.Record) error {
	m, err := json.Marshal(l)
	if err == nil {
		_, err := fmt.Fprintf(os.Stderr, "%s", m)
		return err
	}
	return err
}

type kubeStream struct {
	// the k8s log stream
	stream chan log.Record
	// the stop chan
	stop chan bool
}

func (k *kubeStream) Chan() <-chan log.Record {
	return k.stream
}

func (k *kubeStream) Stop() error {
	select {
	case <-k.stop:
		return nil
	default:
		close(k.stop)
		close(k.stream)
	}
	return nil
}
