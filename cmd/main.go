package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/open-policy-agent/cert-controller/pkg/rotator"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/admission"
	"github.com/tommyskeff/lameduck-controller/internal/controller"
	"github.com/tommyskeff/lameduck-controller/internal/fleet"
	"github.com/tommyskeff/lameduck-controller/internal/metrics"
	"github.com/tommyskeff/lameduck-controller/internal/revision"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lameduckv1alpha1.AddToScheme(scheme))
}

type config struct {
	metricsAddr          string
	probeAddr            string
	secureMetrics        bool
	enableHTTP2          bool
	enableLeaderElection bool
	webhookCertPath      string
	webhookCertName      string
	webhookCertKey       string
	webhookPort          int
	webhookSecret        string
	webhookService       string
	webhookConfiguration string
	namespace            string
	podName              string
	fleetBindAddress     string
	fleetAddress         string
	fleetCertSecret      string
	fleetCertPath        string
	fleetAcceptPlaintext bool
	sidecarImage         string
	sidecarAddress       string
	sidecarPort          int32
	controllerUsername   string
	zap                  zap.Options
}

func loadConfig() (config, error) {
	var cfg config
	flag.StringVar(&cfg.metricsAddr, "metrics-bind-address", "0", "Metrics address; 0 disables metrics.")
	flag.StringVar(&cfg.probeAddr, "health-probe-bind-address", ":8081", "Health probe address.")
	flag.BoolVar(&cfg.enableLeaderElection, "leader-elect", false, "Enable leader election.")
	flag.BoolVar(&cfg.secureMetrics, "metrics-secure", true, "Serve metrics over HTTPS.")
	flag.BoolVar(&cfg.enableHTTP2, "enable-http2", false, "Enable HTTP/2 for the metrics and webhook servers.")
	flag.StringVar(&cfg.webhookCertPath, "webhook-cert-path", "", "Directory holding the webhook certificate.")
	flag.StringVar(&cfg.webhookCertName, "webhook-cert-name", "tls.crt", "Webhook certificate file name.")
	flag.StringVar(&cfg.webhookCertKey, "webhook-cert-key", "tls.key", "Webhook key file name.")
	flag.IntVar(&cfg.webhookPort, "webhook-port", 9443, "Webhook port; -1 disables webhooks.")
	flag.StringVar(&cfg.webhookSecret, "webhook-cert-secret", "",
		"Secret holding the self-signed webhook certificate; empty leaves certificates to the cluster.")
	flag.StringVar(&cfg.webhookService, "webhook-service", "", "Service name the webhook certificate is issued for.")
	flag.StringVar(&cfg.webhookConfiguration, "webhook-configuration", "", "Webhook configuration to inject the CA into.")
	flag.StringVar(&cfg.namespace, "namespace", "", "Namespace the controller runs in.")
	flag.StringVar(&cfg.podName, "pod-name", "",
		"Name of the controller's own pod, labelled while it leads; empty disables the label.")
	flag.StringVar(&cfg.fleetBindAddress, "fleet-bind-address", ":9500", "Address the fleet gRPC server listens on.")
	flag.StringVar(&cfg.fleetAddress, "fleet-address", "", "Address sidecars dial to reach the fleet server.")
	flag.StringVar(&cfg.fleetCertSecret, "fleet-cert-secret", "", "Secret holding the self-signed fleet certificate.")
	flag.StringVar(&cfg.fleetCertPath, "fleet-cert-path", "", "Directory the fleet certificate secret is mounted at.")
	flag.BoolVar(&cfg.fleetAcceptPlaintext, "fleet-accept-plaintext", false,
		"Also accept plaintext connections from sidecars injected before the fleet server used TLS.")
	flag.StringVar(&cfg.sidecarImage, "sidecar-image", "", "Image injected as the sidecar.")
	flag.StringVar(&cfg.sidecarAddress, "sidecar-address",
		fmt.Sprintf("127.0.0.1:%d", lameduckv1alpha1.DefaultSidecarPort), "Sidecar SDK address in each pod.")
	flag.StringVar(&cfg.controllerUsername, "controller-username", "", "Username exempt from delete protection.")
	cfg.zap.BindFlags(flag.CommandLine)
	flag.Parse()

	if cfg.fleetAddress == "" {
		return config{}, errors.New("--fleet-address is required")
	}
	if cfg.sidecarImage == "" {
		return config{}, errors.New("--sidecar-image is required")
	}
	if cfg.fleetCertSecret == "" || cfg.fleetCertPath == "" || cfg.namespace == "" {
		return config{}, errors.New("--fleet-cert-secret, --fleet-cert-path and --namespace are required")
	}
	_, port, err := net.SplitHostPort(cfg.sidecarAddress)
	if err != nil {
		return config{}, fmt.Errorf("--sidecar-address is invalid: %w", err)
	}
	sidecarPort, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		return config{}, fmt.Errorf("--sidecar-address port is invalid: %w", err)
	}
	cfg.sidecarPort = int32(sidecarPort)
	if cfg.webhookSecret != "" {
		if cfg.webhookCertPath == "" || cfg.webhookService == "" || cfg.webhookConfiguration == "" {
			return config{}, errors.New(
				"--webhook-cert-secret needs --webhook-cert-path, --webhook-service and --webhook-configuration")
		}
	}
	return cfg, nil
}

func main() {
	cfg, err := loadConfig()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&cfg.zap)))
	if err != nil {
		setupLog.Error(err, "Invalid configuration")
		os.Exit(1)
	}

	var tlsOpts []func(*tls.Config)
	if !cfg.enableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			c.NextProtos = []string{"http/1.1"}
		})
	}

	webhookOptions := webhook.Options{
		TLSOpts: tlsOpts,
		Port:    cfg.webhookPort,
	}
	if cfg.webhookCertPath != "" {
		webhookOptions.CertDir = cfg.webhookCertPath
		webhookOptions.CertName = cfg.webhookCertName
		webhookOptions.KeyName = cfg.webhookCertKey
	}

	metricsOptions := metricsserver.Options{
		BindAddress:   cfg.metricsAddr,
		SecureServing: cfg.secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if cfg.secureMetrics {
		metricsOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	cacheOptions, err := controller.CacheOptions()
	if err != nil {
		setupLog.Error(err, "Failed to build cache options")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Cache:                         cacheOptions,
		Metrics:                       metricsOptions,
		WebhookServer:                 webhook.NewServer(webhookOptions),
		HealthProbeBindAddress:        cfg.probeAddr,
		LeaderElection:                cfg.enableLeaderElection,
		LeaderElectionID:              "lameduck-controller.tommyjs.dev",
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to create manager")
		os.Exit(1)
	}

	realClock := clock.RealClock{}
	registry := fleet.NewRegistry(realClock)
	drainMetrics := metrics.NewDrainMetrics()
	if err := drainMetrics.Register(crmetrics.Registry); err != nil {
		setupLog.Error(err, "Failed to register metrics")
		os.Exit(1)
	}
	if err := crmetrics.Registry.Register(metrics.SidecarCollector{Counter: registry}); err != nil {
		setupLog.Error(err, "Failed to register sidecar metrics")
		os.Exit(1)
	}
	recorder := mgr.GetEventRecorder("lameduck-controller")

	fleetCertsReady := make(chan struct{})
	fleetRotator, err := fleet.NewCertificateRotator(
		types.NamespacedName{Namespace: cfg.namespace, Name: cfg.fleetCertSecret},
		cfg.fleetCertPath, cfg.fleetAddress, fleetCertsReady)
	if err != nil {
		setupLog.Error(err, "Invalid fleet certificate configuration")
		os.Exit(1)
	}
	if err := rotator.AddRotator(mgr, fleetRotator); err != nil {
		setupLog.Error(err, "Failed to add fleet certificate rotator")
		os.Exit(1)
	}

	err = mgr.Add(&fleet.Server{
		Address:           cfg.fleetBindAddress,
		CertificateDir:    cfg.fleetCertPath,
		CertificatesReady: fleetCertsReady,
		AcceptPlaintext:   cfg.fleetAcceptPlaintext,
		Registry:          registry,
		Authenticator: fleet.NewTokenAuthenticator(mgr.GetClient(), realClock, fleet.ReviewLimits{
			Concurrency:     16,
			Timeout:         10 * time.Second,
			CacheTTL:        10 * time.Minute,
			FailureInterval: 10 * time.Second,
			FailureBurst:    5,
		}),
		Reader: mgr.GetClient(),
		Log:    ctrl.Log.WithName("fleet"),
	})
	if err != nil {
		setupLog.Error(err, "Failed to add fleet server")
		os.Exit(1)
	}

	if cfg.podName != "" {
		err = mgr.Add(&fleet.LeaderLabeler{
			Reader: mgr.GetAPIReader(),
			Writer: mgr.GetClient(),
			Pod:    types.NamespacedName{Namespace: cfg.namespace, Name: cfg.podName},
			Log:    ctrl.Log.WithName("leader"),
		})
		if err != nil {
			setupLog.Error(err, "Failed to add leader labeler")
			os.Exit(1)
		}
	}

	reconciler := &controller.DrainSetReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Clock:    realClock,
		Registry: registry,
		History:  revision.History{Client: mgr.GetClient(), Scheme: mgr.GetScheme()},
		Pods: controller.PodFactory{
			Scheme:         mgr.GetScheme(),
			SidecarImage:   cfg.sidecarImage,
			SidecarAddress: cfg.sidecarAddress,
			FleetAddress:   cfg.fleetAddress,
			FleetCA:        fleet.CertificateAuthorityIn(cfg.fleetCertPath),
		},
		Expectations: controller.NewExpectations(realClock),
		Recorder:     recorder,
		Metrics:      drainMetrics,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller")
		os.Exit(1)
	}

	webhooksReady := make(chan struct{})
	if cfg.webhookPort > 0 {
		certsReady := make(chan struct{})
		if cfg.webhookSecret != "" {
			if err := addCertRotator(mgr, cfg, certsReady); err != nil {
				setupLog.Error(err, "Failed to add certificate rotator")
				os.Exit(1)
			}
		} else {
			close(certsReady)
		}
		go func() {
			<-certsReady
			if err := registerWebhooks(mgr, cfg, recorder, drainMetrics); err != nil {
				setupLog.Error(err, "Failed to register webhooks")
				os.Exit(1)
			}
			close(webhooksReady)
		}()
	} else {
		close(webhooksReady)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", channelChecker(webhooksReady)); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

func addCertRotator(mgr manager.Manager, cfg config, ready chan struct{}) error {
	service := fmt.Sprintf("%s.%s.svc", cfg.webhookService, cfg.namespace)
	return rotator.AddRotator(mgr, &rotator.CertRotator{
		SecretKey:      types.NamespacedName{Namespace: cfg.namespace, Name: cfg.webhookSecret},
		CertDir:        cfg.webhookCertPath,
		CertName:       cfg.webhookCertName,
		KeyName:        cfg.webhookCertKey,
		CAName:         "lameduck-ca",
		CAOrganization: "lameduck",
		DNSName:        service,
		ExtraDNSNames:  []string{service + ".cluster.local"},
		IsReady:        ready,
		Webhooks:       []rotator.WebhookInfo{{Name: cfg.webhookConfiguration, Type: rotator.Validating}},
		FieldOwner:     "lameduck-controller",
	})
}

func registerWebhooks(
	mgr manager.Manager,
	cfg config,
	recorder events.EventRecorder,
	drainMetrics *metrics.DrainMetrics,
) error {
	validator := &admission.DrainSetValidator{SidecarPort: cfg.sidecarPort}
	if err := admission.SetupDrainSetValidator(mgr, validator); err != nil {
		return fmt.Errorf("set up drainset validator: %w", err)
	}
	server := mgr.GetWebhookServer()
	server.Register(admission.EvictionPath, &webhook.Admission{Handler: &admission.DisruptionHandler{
		Client:             mgr.GetClient(),
		ControllerUsername: cfg.controllerUsername,
		Eviction:           true,
		Recorder:           recorder,
		Metrics:            drainMetrics,
	}})
	server.Register(admission.DeletePath, &webhook.Admission{Handler: &admission.DisruptionHandler{
		Client:             mgr.GetClient(),
		ControllerUsername: cfg.controllerUsername,
		Recorder:           recorder,
		Metrics:            drainMetrics,
	}})
	return nil
}

func channelChecker(ready <-chan struct{}) healthz.Checker {
	return func(*http.Request) error {
		select {
		case <-ready:
			return nil
		default:
			return errors.New("webhooks are not registered")
		}
	}
}
