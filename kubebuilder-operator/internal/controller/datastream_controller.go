package controller

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	messagingv1alpha1 "github.com/lorenzodeluca/kafka-operator-clash/kubebuilder-operator/api/v1alpha1"
)

const (
	defaultKafkaBroker = "kafka.kafka.svc.cluster.local:9092"
)

// DataStreamReconciler reconciles a DataStream object
type DataStreamReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=messaging.kb.lorenzodeluca.it,resources=datastreams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=messaging.kb.lorenzodeluca.it,resources=datastreams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

func (r *DataStreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var ds messagingv1alpha1.DataStream
	if err := r.Get(ctx, req.NamespacedName, &ds); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if ds.Spec.TopicName == "" {
		r.setReadyCondition(ctx, &ds, metav1.ConditionFalse, "InvalidSpec", "spec.topicName must not be empty")
		return ctrl.Result{}, nil
	}

	partitions := ds.Spec.Partitions
	if partitions <= 0 {
		partitions = 1
	}
	replication := ds.Spec.ReplicationFactor
	if replication <= 0 {
		replication = 1
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = defaultKafkaBroker
	}

	logger.Info("Ensuring Kafka topic exists", "topic", ds.Spec.TopicName, "broker", kafkaBroker)
	if err := createKafkaTopic(kafkaBroker, ds.Spec.TopicName, partitions, replication); err != nil {
		r.setReadyCondition(ctx, &ds, metav1.ConditionFalse, "KafkaError", err.Error())
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	configMapName := ds.Name + "-connection"
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: ds.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{
			"KAFKA_BROKER": kafkaBroker,
			"KAFKA_TOPIC":  ds.Spec.TopicName,
		}
		return ctrl.SetControllerReference(&ds, cm, r.Scheme)
	})
	if err != nil {
		r.setReadyCondition(ctx, &ds, metav1.ConditionFalse, "ConfigMapError", err.Error())
		return ctrl.Result{}, err
	}

	deployName := ds.Name + "-consumer"
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: ds.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		deploy.Spec.Replicas = &partitions
		deploy.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": deployName},
		}
		deploy.Spec.Template.ObjectMeta.Labels = map[string]string{"app": deployName}
		deploy.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "consumer",
				Image:   "alpine:3.20",
				Command: []string{"sh", "-c", `echo "Broker=$KAFKA_BROKER Topic=$KAFKA_TOPIC"; sleep infinity`},
				EnvFrom: []corev1.EnvFromSource{
					{
						ConfigMapRef: &corev1.ConfigMapEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
						},
					},
				},
			},
		}
		return ctrl.SetControllerReference(&ds, deploy, r.Scheme)
	})
	if err != nil {
		r.setReadyCondition(ctx, &ds, metav1.ConditionFalse, "DeploymentError", err.Error())
		return ctrl.Result{}, err
	}

	ds.Status.TopicCreated = true
	ds.Status.ConfigMapRef = configMapName
	r.setReadyCondition(ctx, &ds, metav1.ConditionTrue, "Ready", "Topic, ConfigMap and Deployment are reconciled")

	if err := r.Status().Update(ctx, &ds); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *DataStreamReconciler) setReadyCondition(ctx context.Context, ds *messagingv1alpha1.DataStream, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&ds.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             status,
		Reason:             reason,
		Message:            msg,
		LastTransitionTime: metav1.Now(),
	})
	_ = r.Status().Update(ctx, ds)
}

func createKafkaTopic(broker, topic string, partitions int32, reps int16) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("dial broker failed: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("cannot get kafka controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		// Fallback for local-host testing (port-forward / no cluster DNS on host).
		controllerConn, err = kafka.Dial("tcp", broker)
		if err != nil {
			return fmt.Errorf("cannot dial kafka controller (%s) nor broker fallback (%s): %w", controllerAddr, broker, err)
		}
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     int(partitions),
		ReplicationFactor: int(reps),
	})
	if err != nil {
		l := strings.ToLower(err.Error())
		if strings.Contains(l, "already exists") || strings.Contains(l, "topic already exists") {
			return nil
		}
		return err
	}
	return nil
}

func (r *DataStreamReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&messagingv1alpha1.DataStream{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}
