/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/connection"
	"github.com/kubernetes-csi/csi-lib-utils/leaderelection"
	"github.com/kubernetes-csi/csi-lib-utils/metrics"
	"github.com/kubernetes-csi/csi-lib-utils/rpc"
	"github.com/kubernetes-csi/external-attacher/pkg/attacher"
	"github.com/kubernetes-csi/external-attacher/pkg/controller"
	"google.golang.org/grpc"
)

const (

	// Default timeout of short CSI calls like GetPluginInfo
	csiTimeout = time.Second

	leaderElectionTypeLeases = "leases"
)

// Command line flags
var (
	kubeconfig    = flag.String("kubeconfig", "", "Absolute path to the kubeconfig file. Required only when running out of cluster.")
	resync        = flag.Duration("resync", 10*time.Minute, "Resync interval of the controller.")
	csiAddress    = flag.String("csi-address", "/run/csi/socket", "Address of the CSI driver socket.")
	showVersion   = flag.Bool("version", false, "Show version.")
	timeout       = flag.Duration("timeout", 15*time.Second, "Timeout for waiting for attaching or detaching the volume.")
	workerThreads = flag.Uint("worker-threads", 10, "Number of attacher worker threads")

	retryIntervalStart = flag.Duration("retry-interval-start", time.Second, "Initial retry interval of failed create volume or deletion. It doubles with each failure, up to retry-interval-max.")
	retryIntervalMax   = flag.Duration("retry-interval-max", 5*time.Minute, "Maximum retry interval of failed create volume or deletion.")

	enableLeaderElection    = flag.Bool("leader-election", false, "Enable leader election.")
	leaderElectionNamespace = flag.String("leader-election-namespace", "", "Namespace where the leader election resource lives. Defaults to the pod namespace if not set.")

	reconcileSync = flag.Duration("reconcile-sync", 1*time.Minute, "Resync interval of the VolumeAttachment reconciler.")

	metricsAddress = flag.String("metrics-address", "", "(deprecated) The TCP network address where the prometheus metrics endpoint will listen (example: `:8080`). The default is empty string, which means metrics endpoint is disabled. Only one of `--metrics-address` and `--http-endpoint` can be set.")
	httpEndpoint   = flag.String("http-endpoint", "", "The TCP network address where the HTTP server for diagnostics, including metrics and leader election health check, will listen (example: `:8080`). The default is empty string, which means the server is disabled. Only one of `--metrics-address` and `--http-endpoint` can be set.")
	metricsPath    = flag.String("metrics-path", "/metrics", "The HTTP path where prometheus metrics will be exposed. Default is `/metrics`.")

	kubeAPIQPS   = flag.Float64("kube-api-qps", 5, "QPS to use while communicating with the kubernetes apiserver. Defaults to 5.0.")
	kubeAPIBurst = flag.Int("kube-api-burst", 10, "Burst to use while communicating with the kubernetes apiserver. Defaults to 10.")
)

var (
	version = "unknown"
)

type leaderElection interface {
	Run() error
	WithNamespace(namespace string)
}

func main() {
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()

	if *showVersion {
		fmt.Println(os.Args[0], version)
		return
	}
	klog.Infof("Version: %s", version)

	if *metricsAddress != "" && *httpEndpoint != "" {
		klog.Error("only one of `--metrics-address` and `--http-endpoint` can be set.")
		os.Exit(1)
	}
	addr := *metricsAddress
	if addr == "" {
		addr = *httpEndpoint
	}

	// Create the client config. Use kubeconfig if given, otherwise assume in-cluster.
	config, err := buildConfig(*kubeconfig)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}
	config.QPS = (float32)(*kubeAPIQPS)
	config.Burst = *kubeAPIBurst

	if *workerThreads == 0 {
		klog.Error("option -worker-threads must be greater than zero")
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}

	factory := informers.NewSharedInformerFactory(clientset, *resync)
	var handler controller.Handler
	metricsManager := metrics.NewCSIMetricsManager("" /* driverName */)

	// Connect to CSI.
	csiConn, err := connection.Connect(*csiAddress, metricsManager, connection.OnConnectionLoss(connection.ExitOnConnectionLoss()))
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}

	err = rpc.ProbeForever(csiConn, *timeout)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}

	// Find driver name.
	ctx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	defer cancel()
	csiAttacher, err := rpc.GetDriverName(ctx, csiConn)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}
	klog.V(2).Infof("CSI driver name: %q", csiAttacher)

	translator := csitrans.New()
	if translator.IsMigratedCSIDriverByName(csiAttacher) {
		metricsManager = metrics.NewCSIMetricsManagerWithOptions(csiAttacher, metrics.WithMigration())
		migratedCsiClient, err := connection.Connect(*csiAddress, metricsManager, connection.OnConnectionLoss(connection.ExitOnConnectionLoss()))
		if err != nil {
			klog.Error(err.Error())
			os.Exit(1)
		}
		csiConn.Close()
		csiConn = migratedCsiClient

		err = rpc.ProbeForever(csiConn, *timeout)
		if err != nil {
			klog.Error(err.Error())
			os.Exit(1)
		}
	}

	// Prepare http endpoint for metrics + leader election healthz
	mux := http.NewServeMux()
	if addr != "" {
		metricsManager.RegisterToServer(mux, *metricsPath)
		metricsManager.SetDriverName(csiAttacher)
		go func() {
			klog.Infof("ServeMux listening at %q", addr)
			err := http.ListenAndServe(addr, mux)
			if err != nil {
				klog.Fatalf("Failed to start HTTP server at specified address (%q) and metrics path (%q): %s", addr, *metricsPath, err)
			}
		}()
	}

	supportsService, err := supportsPluginControllerService(ctx, csiConn)
	if err != nil {
		klog.Error(err.Error())
		os.Exit(1)
	}
	if !supportsService {
		handler = controller.NewTrivialHandler(clientset)
		klog.V(2).Infof("CSI driver does not support Plugin Controller Service, using trivial handler")
	} else {
		// Find out if the driver supports attach/detach.
		supportsAttach, supportsReadOnly, err := supportsControllerPublish(ctx, csiConn)
		if err != nil {
			klog.Error(err.Error())
			os.Exit(1)
		}
		if supportsAttach {
			poLister := factory.Core().V1().Pods().Lister()
			pvLister := factory.Core().V1().PersistentVolumes().Lister()
			vaLister := factory.Storage().V1().VolumeAttachments().Lister()
			csiNodeLister := factory.Storage().V1().CSINodes().Lister()
			volAttacher := attacher.NewAttacher(csiConn)
			CSIVolumeLister := attacher.NewVolumeLister(csiConn)
			handler = controller.NewCSIHandler(clientset, csiAttacher, volAttacher, poLister, CSIVolumeLister, pvLister, csiNodeLister, vaLister, timeout, supportsReadOnly, csitrans.New())
			klog.V(2).Infof("CSI driver supports ControllerPublishUnpublish, using real CSI handler")
		} else {
			handler = controller.NewTrivialHandler(clientset)
			klog.V(2).Infof("CSI driver does not support ControllerPublishUnpublish, using trivial handler")
		}
	}

	slvpn, err := supportsListVolumesPublishedNodes(ctx, csiConn)
	if err != nil {
		klog.Errorf("Failed to check if driver supports ListVolumesPublishedNodes, assuming it does not: %v", err)
	}

	if slvpn {
		klog.V(2).Infof("CSI driver supports list volumes published nodes. Using capability to reconcile volume attachment objects with actual backend state")
	}

	ctrl := controller.NewCSIAttachController(
		clientset,
		csiAttacher,
		handler,
		factory.Storage().V1().VolumeAttachments(),
		factory.Core().V1().PersistentVolumes(),
		factory.Core().V1().Pods(),
		workqueue.NewItemExponentialFailureRateLimiter(*retryIntervalStart, *retryIntervalMax),
		workqueue.NewItemExponentialFailureRateLimiter(*retryIntervalStart, *retryIntervalMax),

		slvpn,
		*reconcileSync,
	)

	run := func(ctx context.Context) {
		stopCh := ctx.Done()
		factory.Start(stopCh)
		ctrl.Run(int(*workerThreads), stopCh)
	}

	if !*enableLeaderElection {
		run(context.TODO())
	} else {
		// Create a new clientset for leader election. When the attacher
		// gets busy and its client gets throttled, the leader election
		// can proceed without issues.
		leClientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			klog.Fatalf("Failed to create leaderelection client: %v", err)
		}

		// Name of config map with leader election lock
		lockName := "external-attacher-leader-" + csiAttacher
		le := leaderelection.NewLeaderElection(leClientset, lockName, run)
		if *httpEndpoint != "" {
			le.PrepareHealthCheck(mux, leaderelection.DefaultHealthCheckTimeout)
		}

		if *leaderElectionNamespace != "" {
			le.WithNamespace(*leaderElectionNamespace)
		}

		if err := le.Run(); err != nil {
			klog.Fatalf("failed to initialize leader election: %v", err)
		}
	}
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func supportsControllerPublish(ctx context.Context, csiConn *grpc.ClientConn) (supportsControllerPublish bool, supportsPublishReadOnly bool, err error) {
	caps, err := rpc.GetControllerCapabilities(ctx, csiConn)
	if err != nil {
		return false, false, err
	}

	supportsControllerPublish = caps[csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME]
	supportsPublishReadOnly = caps[csi.ControllerServiceCapability_RPC_PUBLISH_READONLY]
	return supportsControllerPublish, supportsPublishReadOnly, nil
}

func supportsListVolumesPublishedNodes(ctx context.Context, csiConn *grpc.ClientConn) (bool, error) {
	caps, err := rpc.GetControllerCapabilities(ctx, csiConn)
	if err != nil {
		return false, fmt.Errorf("failed to get controller capabilities: %v", err)
	}

	return caps[csi.ControllerServiceCapability_RPC_LIST_VOLUMES] && caps[csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES], nil
}

func supportsPluginControllerService(ctx context.Context, csiConn *grpc.ClientConn) (bool, error) {
	caps, err := rpc.GetPluginCapabilities(ctx, csiConn)
	if err != nil {
		return false, err
	}

	return caps[csi.PluginCapability_Service_CONTROLLER_SERVICE], nil
}
