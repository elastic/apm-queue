// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

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

// New creates a port-forwarder for the give port forwarding request
// and returns once the ports are ready. If the ports fail to be
// ready it returns an error. The resulting forwarder can be run by
// calling `ForwardPorts`. The forwarder will stop after the `stopCh`
// is closed.
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
		return nil, fmt.Errorf("failed to get pod for the give service: %w", err)
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
