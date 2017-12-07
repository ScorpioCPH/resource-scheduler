/*
Copyright 2017 The Caicloud Authors.

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

package scheduler

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	// appsv1beta2 "k8s.io/api/apps/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const defaultSchedulerName = "resource-scheduler"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Pod is synced
	SuccessSynced = "Synced"

	// MessageResourceSynced is the message used for an Event fired when a Pod
	// is synced successfully
	MessageResourceSynced = "Pod synced successfully"
)

// Scheduler is the scheduler implementation for Pod resources
type Scheduler struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	podsLister    corelisters.PodLister
	podsSynced    cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder      record.EventRecorder
	schedulerName string
}

// NewScheduler returns a new sample scheduler
func NewScheduler(
	kubeclientset kubernetes.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory) *Scheduler {

	// obtain references to shared index informers for the Pod types.
	podInformer := kubeInformerFactory.Core().V1().Pods()

	// Create event broadcaster
	glog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: defaultSchedulerName})

	scheduler := &Scheduler{
		kubeclientset: kubeclientset,
		podsLister:    podInformer.Lister(),
		podsSynced:    podInformer.Informer().HasSynced,
		workqueue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Pods"),
		recorder:      recorder,
		schedulerName: defaultSchedulerName,
	}

	glog.Info("Setting up event handlers")
	// Set up an event handler for when Pod resources change. This
	// handler will lookup the owner of the given Pod, and if it is
	// owned by a Pod resource will enqueue that Pod resource for
	// processing. This way, we don't need to implement custom logic for
	// handling Pod resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/schedulers.md
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: scheduler.handleObject,
		UpdateFunc: func(old, new interface{}) {
			glog.Infof("Update Pod")
			newPod := new.(*corev1.Pod)
			oldPod := old.(*corev1.Pod)
			if newPod.ResourceVersion == oldPod.ResourceVersion {
				glog.Infof("ResourceVersion not changed: %s", newPod.ResourceVersion)
				// Periodic resync will send update events for all known Pods.
				// Two different versions of the same Pod will always have different RVs.
				return
			}
			scheduler.handleObject(new)
		},
		DeleteFunc: scheduler.handleObject,
	})

	return scheduler
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (s *Scheduler) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer s.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	glog.Info("Starting Pod scheduler")

	// Wait for the caches to be synced before starting workers
	glog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, s.podsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers")
	// Launch two workers to process Pod resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(s.runWorker, time.Second, stopCh)
	}

	glog.Info("Started workers")
	<-stopCh
	glog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (s *Scheduler) runWorker() {
	for s.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (s *Scheduler) processNextWorkItem() bool {
	obj, shutdown := s.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer s.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer s.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			s.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Pod resource to be synced.
		if err := s.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		s.workqueue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Pod resource
// with the current status of the resource.
func (s *Scheduler) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	glog.Infof("syncHandler, namespace: %s, name: %s", namespace, name)

	// Get the Pod resource with this namespace/name
	pod, err := s.podsLister.Pods(namespace).Get(name)
	if err != nil {
		// The Pod resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("pod '%s' in work queue no longer exists", key))
			return nil
		}

		return err
	}

	glog.Infof("syncHandler, nodeSelector: %+v", pod.Spec.NodeSelector)
	if len(pod.Spec.NodeSelector) == 0 {
		glog.V(4).Infof("no nodeSelector specified, forget this pod '%s'", pod.Name)
		return nil
	}

	// only support one NodeSelectorTerm now

	selector := labels.SelectorFromSet(pod.Spec.NodeSelector)
	scheduled := false

	nodeList, _ := s.kubeclientset.CoreV1().Nodes().List(metav1.ListOptions{})
	for _, node := range nodeList.Items {
		// TODO(cph): more scheduling algorithm here
		if selector.Matches(labels.Set(node.Labels)) {
			// bind Pod and Node here
			b := &corev1.Binding{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      name,
				},
				Target: corev1.ObjectReference{
					Kind: "Node",
					Name: node.Name,
				},
			}
			err := s.kubeclientset.CoreV1().Pods(namespace).Bind(b)
			if err != nil {
				glog.Errorf("syncHandler, Bind err: %s", err)
				return err
			}
			scheduled = true
		}
	}

	glog.Infof("syncHandler, scheduled: %s", scheduled)

	// TODO(cph): more event here
	s.recorder.Event(pod, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

// enqueuePod takes a Pod resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Pod.
func (s *Scheduler) enqueuePod(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	glog.Infof("enqueuePod key: %s", key)
	s.workqueue.AddRateLimited(key)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Pod resource that which schedulerName is equal to defaultSchedulerName.
// It does this by looking at the
// Pod.Spes.SchedulerName field for an appropriate SchedulerName.
// It then enqueues that Pod resource to be processed. If the object does not
// have an appropriate SchedulerName, it will simply be skipped.
func (s *Scheduler) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		glog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	glog.V(4).Infof("Processing object: %s", object.GetName())

	pod, err := s.podsLister.Pods(object.GetNamespace()).Get(object.GetName())
	if err != nil {
		glog.V(4).Infof("ignoring orphaned object '%s' of pod '%s'", object.GetSelfLink(), object.GetName())
		return
	}

	if pod.Spec.SchedulerName != defaultSchedulerName {
		glog.V(4).Infof("schedulerName don't match, ignoring object '%s' of pod '%s'", object.GetSelfLink(), object.GetName())
		return
	}

	// TODO(cph): find a proper way to check whether this Pod is scheduled.
	if pod.Status.Phase != corev1.PodPending {
		glog.V(4).Infof("this Pod '%s' is scheduled, ignoring", object.GetName())
		return
	}

	s.enqueuePod(pod)
	return
}
