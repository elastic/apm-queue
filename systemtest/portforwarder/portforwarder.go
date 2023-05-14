// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package portforwarder

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	homedir "github.com/mitchellh/go-homedir"
	k8sv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// Request is a request to port forward a resource in k8s cluster.
type Request struct {
	KubeCfg     string
	ServiceName string
	Namespace   string
	PortMapping string
}

// New creates a port-forwarder for the given port forwarding request.
// The returned port-forwarder can be started by calling `ForwardPorts`.
// The forwarder will stop after the `stopCh` is closed. `Ready` field
// in the returned forwarder can be used to check the readiness of the
// forwarded ports, `Ready` will be closed when the ports are ready.
func (r Request) New(
	ctx context.Context,
	stopCh chan struct{},
) (*portforward.PortForwarder, error) {
	// Ensure that `~` is exapnded to home dir if applicable
	kubeCfg, _ := homedir.Expand(r.KubeCfg)
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kube config: %w", err)
	}

	k8sAPIServerHost, err := url.Parse(cfg.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to parse k8s apiserver host: %w", err)
	}

	pod, err := r.getPod(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod for service %s: %w", r.ServiceName, err)
	}

	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}
	dialer := spdy.NewDialer(
		upgrader,
		&http.Client{Transport: transport},
		http.MethodPost,
		&url.URL{
			Scheme: k8sAPIServerHost.Scheme,
			Path:   fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", pod.Namespace, pod.Name),
			Host:   k8sAPIServerHost.Host,
		},
	)
	return portforward.New(
		dialer,
		[]string{r.PortMapping},
		stopCh,
		make(chan struct{}),
		os.Stdout,
		os.Stderr,
	)
}

func (r Request) getPod(ctx context.Context, cfg *rest.Config) (*k8sv1.Pod, error) {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	svc, err := clientset.CoreV1().
		Services(r.Namespace).
		Get(ctx, r.ServiceName, metav1.GetOptions{})
	if err != nil || svc == nil {
		return nil, fmt.Errorf("failed to find the service: %w", err)
	}

	labels := []string{}
	for key, val := range svc.Spec.Selector {
		labels = append(labels, key+"="+val)
	}
	pods, err := clientset.CoreV1().
		Pods(r.Namespace).
		List(ctx, metav1.ListOptions{
			LabelSelector: strings.Join(labels, ","),
			Limit:         1,
		})
	if err != nil || len(pods.Items) == 0 {
		return nil, fmt.Errorf("failed to find any pod for the service: %w", err)
	}

	return &pods.Items[0], nil
}
