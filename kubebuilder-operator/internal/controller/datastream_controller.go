package controller

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	messagingv1alpha1 "github.com/lorenzodeluca/kafka-operator-clash/kubebuilder-operator/api/v1alpha1"
)

// DataStreamReconciler reconciles a DataStream object
type DataStreamReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// RBAC Permissions necessary for our operator
// +kubebuilder:rbac:groups=messaging.lorenzodeluca.it,resources=datastreams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=messaging.lorenzodeluca.it,resources=datastreams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

func (r *DataStreamReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the DataStream instance
	var datastream messagingv1alpha1.DataStream
	if err := r.Get(ctx, req.NamespacedName, &datastream); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	kafkaBroker := "kafka.kafka.svc.cluster.local:9092"

	// 2. Provision the Kafka Topic
	logger.Info("Ensuring Kafka topic exists", "topic", datastream.Spec.TopicName)
	err := createKafkaTopic(kafkaBroker, datastream.Spec.TopicName, datastream.Spec.Partitions, datastream.Spec.ReplicationFactor)
	if err != nil {
		logger.Error(err, "Failed to create Kafka topic")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// 3. Ensure ConfigMap exists
	configMapName := datastream.Name + "-connection"
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: datastream.Namespace,
		},
		Data: map[string]string{
			"KAFKA_BROKER": kafkaBroker,
			"KAFKA_TOPIC":  datastream.Spec.TopicName,
		},
	}
	// Bind ownership to automate cleanup when DataStream is deleted
	if err := ctrl.SetControllerReference(&datastream, cm, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingCM corev1.ConfigMap
	err = r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: datastream.Namespace}, &existingCM)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating connection ConfigMap", "name", configMapName)
		if err := r.Create(ctx, cm); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 4. Ensure Consumer Deployment exists
	deployName := datastream.Name + "-consumer"
	var partitions int32 = 1
	if datastream.Spec.Partitions > 0 {
		partitions = datastream.Spec.Partitions
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: datastream.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &partitions, // One consumer pod per partition
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deployName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deployName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "consumer",
							Image: "alpine", // Lightweight mock app for testing env
							Command: []string{
								"sh", "-c",
								"echo 'Connecting to '$KAFKA_BROKER' tracking topic '$KAFKA_TOPIC; sleep infinity",
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: configMapName},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(&datastream, deployment, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	var existingDeploy appsv1.Deployment
	err = r.Get(ctx, types.NamespacedName{Name: deployName, Namespace: datastream.Namespace}, &existingDeploy)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating Consumer Deployment", "name", deployName)
		if err := r.Create(ctx, deployment); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 5. Update Status
	if !datastream.Status.TopicCreated || datastream.Status.ConfigMapRef != configMapName {
		datastream.Status.TopicCreated = true
		datastream.Status.ConfigMapRef = configMapName
		if err := r.Status().Update(ctx, &datastream); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func createKafkaTopic(broker, topic string, partitions int32, reps int16) error {
	if partitions <= 0 {
		partitions = 1
	}
	if reps <= 0 {
		reps = 1
	}

	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     int(partitions),
		ReplicationFactor: int(reps),
	}

	err = conn.CreateTopics(topicConfig)
	if err != nil {
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
