package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/justtrackio/gosoline/pkg/appctx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	eventsv1 "k8s.io/api/events/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	ClientModeInCluster  = "in-cluster"
	ClientModeKubeConfig = "kube-config"
)

type KubeSettings struct {
	ClientMode string `cfg:"client_mode" default:"in-cluster"`
	Context    string `cfg:"context"`
}

type k8sServiceCtxKey struct{}

func ProvideK8sService(ctx context.Context, config cfg.Config, logger log.Logger) (*K8sService, error) {
	return appctx.Provide(ctx, k8sServiceCtxKey{}, func() (*K8sService, error) {
		settings := &KubeSettings{}
		if err := config.UnmarshalKey("kube", settings); err != nil {
			return nil, fmt.Errorf("could not unmarshal kube settings: %w", err)
		}

		if settings.ClientMode == ClientModeInCluster {
			clientConfig, err := rest.InClusterConfig()
			if err != nil {
				return nil, fmt.Errorf("could not load in cluster config: %w", err)
			}

			return newK8sServiceFromConfig(clientConfig, logger)
		}

		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{
			CurrentContext: settings.Context,
		})

		clientConfig, err := loader.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("could not load config: %w", err)
		}

		return newK8sServiceFromConfig(clientConfig, logger)
	})
}

func newK8sServiceFromConfig(clientConfig *rest.Config, logger log.Logger) (*K8sService, error) {
	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create k8s client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("could not create dynamic client: %w", err)
	}

	return &K8sService{
		logger:        logger.WithChannel("k8s"),
		dynamicClient: dynamicClient,
		client:        client,
		namespace:     "lakehouse-admin",
	}, nil
}

type K8sService struct {
	logger        log.Logger
	dynamicClient dynamic.Interface
	client        *kubernetes.Clientset
	namespace     string
}

func (s *K8sService) WatchDeployments(ctx context.Context) (watch.Interface, error) {
	gvr := schema.GroupVersionResource{Group: "flink.apache.org", Version: "v1beta1", Resource: "flinkdeployments"}
	deployments := s.dynamicClient.Resource(gvr)

	return deployments.Watch(ctx, metav1.ListOptions{})
}

func (s *K8sService) GetEvents(ctx context.Context, namespace string, name string) (*eventsv1.EventList, error) {
	fieldSelector := fmt.Sprintf("regarding.name=%s", name)
	events, err := s.client.EventsV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("could not list events for %s/%s: %w", namespace, name, err)
	}

	return events, nil
}

func (s *K8sService) CreateSparkApplication(ctx context.Context, manifest *SparkApplicationManifest) (*SparkApplicationManifest, error) {
	var err error
	var resource, created *unstructured.Unstructured
	var result *SparkApplicationManifest

	if manifest == nil {
		return nil, fmt.Errorf("spark application manifest is required")
	}

	if manifest.Metadata.Namespace == "" {
		return nil, fmt.Errorf("spark application namespace is required")
	}

	if resource, err = manifest.ToCreateUnstructured(); err != nil {
		return nil, fmt.Errorf("could not convert spark application to unstructured: %w", err)
	}

	gvr := schema.GroupVersionResource{Group: "spark.apache.org", Version: "v1", Resource: "sparkapplications"}
	if created, err = s.dynamicClient.Resource(gvr).Namespace(manifest.Metadata.Namespace).Create(ctx, resource, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("could not create spark application %s/%s: %w", manifest.Metadata.Namespace, manifest.Metadata.Name, err)
	}

	if result, err = UnstructuredToSparkApplicationManifest(created); err != nil {
		return nil, fmt.Errorf("could not decode created spark application %s/%s: %w", manifest.Metadata.Namespace, manifest.Metadata.Name, err)
	}

	return result, nil
}

func (s *K8sService) DeleteSparkApplication(ctx context.Context, namespace string, name string) error {
	if name == "" {
		return fmt.Errorf("spark application name is required")
	}

	if namespace == "" {
		namespace = s.namespace
	}

	gvr := schema.GroupVersionResource{Group: "spark.apache.org", Version: "v1", Resource: "sparkapplications"}
	err := s.dynamicClient.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err == nil || k8serrors.IsNotFound(err) {
		return nil
	}

	return fmt.Errorf("could not delete spark application %s/%s: %w", namespace, name, err)
}

func (s *K8sService) WatchSparkApplications(ctx context.Context) (cache.SharedIndexInformer, error) {
	gvr := schema.GroupVersionResource{Group: "spark.apache.org", Version: "v1", Resource: "sparkapplications"}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(s.dynamicClient, time.Minute, s.namespace, nil)
	informer := factory.ForResource(gvr).Informer()

	go informer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil, fmt.Errorf("could not sync spark application informer cache")
	}

	return informer, nil
}
