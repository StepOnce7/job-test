/*
Copyright 2022.

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

package controllers

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sort"
	"time"

	"github.com/robfig/cron"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	batchv1 "kubebuilder_test/job-test/api/v1"
)

var (
	scheduledTimeAnnotation = "batch.tutorial.kubeilder.io/scheduled-at"
	jobOwnerKey             = ".metadata.controller"
	apiGVStr                = batchv1.GroupVersion.String()
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

//+kubebuilder:rbac:groups=batch.tutorial.kubeilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.tutorial.kubeilder.io,resources=cronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.tutorial.kubeilder.io,resources=cronjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.0/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logg := log.FromContext(ctx)

	// TODO(user): your logic here
	// 1. Load the CronJob by name
	var cronJob batchv1.CronJob
	// Get is special, in that it takes a  namespacedName as the middle argument
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		logg.Error(err, "unable to fetch CronJob")
		// we will ignore not-found errors, since they can`t be fixed by an immediate requeue,
		// and we will need to wait for a new notification, and we can get them on deleted requests
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. List all active jobs, and update the status.
	// we will need to list child jobs in this namespace that belong to this CronJob
	var childJobs kbatch.JobList
	// we can use the List method to list the child jobs.
	// notice that we use variadic options to set the namespace and field match
	// and this is an index lookup
	// Because: we wil fetches all jobs owned by the cronjob for the status.
	// and as our number of cronJobs increases, looking these up can become quite slow as we have to filter through all of them.
	// For a more efficient lookup, these jobs will be indexed locally on the controller's name.
	// A jobOwnerKey field is added to the cached job objects.
	// This key references the owning controller and functions as the index.
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		logg.Error(err, "unable to list child Jobs")
		return ctrl.Result{}, err
	}

	// Once we have all the jobs we own, will split them into active, successful, and failed jobs
	// keeping track of the most recent run so that we can record it in status

	// todo: question
	// Remember, status should be able to be reconstituted from the state of the world,
	// so it’s generally not a good idea to read from the status of the root object.
	// Instead, you should reconstruct it every run. That’s what we’ll do here

	// find the active list of jobs
	var activeJobs []*kbatch.Job
	// find the successfulJobs list of jobs
	var successfulJobs []*kbatch.Job
	// find the failed list of jobs
	var failedJobs []*kbatch.Job
	// find the lase run so we can update the status
	var mostRecentTime *time.Time

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "":
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}

		// We'll store the launch time in an annotation, so we'll reconstitute that from
		// the active jobs themselves.
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			logg.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}

		if scheduledTimeForJob != nil {
			if mostRecentTime == nil {
				mostRecentTime = scheduledTimeForJob
			} else if mostRecentTime.Before(*scheduledTimeForJob) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		// update the last time the job was successfully scheduled.
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}

	// A list of pointers to currently running jobs.
	cronJob.Status.Active = nil
	// 遍历活跃状态的 job
	for _, activeJob := range activeJobs {
		// ObjectReference contains enough information to let you inspect or modify the referred object.
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			logg.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		// 指向当前正在运行的 job 的指针列表
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	// Here, we’ll log how many jobs we observed at a slightly higher logging level, for debugging
	logg.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	// Using the date we’ve gathered, we’ll update the status of our CRD
	// Once we’ve updated our status, we can move on to ensuring that the status of the world matches what we want in our spec
	if err := r.Status().Update(ctx, &cronJob); err != nil {
		logg.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	// 3: Clean up old jobs according to the history limit
	// NB: deleting these is "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish the deleting.
	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		// 按照 startTime 升序排列
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})

		//｜0｜----delete----｜len-limit｜----｜len｜
		//｜-----------------｜-------------------｜
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				logg.Error(err, "unable to delete old failed job", "job", job)
			} else {
				logg.V(0).Info("deleted old failed job", "job", job)
			}
		}
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); (err) != nil {
				logg.Error(err, "unable to delete old successful job", "job", job)
			} else {
				logg.V(0).Info("deleted old successful job", "job", job)
			}
		}
	}

	// 4: Check if we’re suspended
	// If this object is suspended, we don’t want to run any jobs, so we’ll stop now
	// This is useful if something’s broken with the job we’re running,
	// and we want to pause runs to investigate or putz with the cluster, without deleting the object.
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		logg.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// 5: Get the next scheduled run
	// If we’re not paused, we’ll need to calculate the next scheduled run, and whether or not we’ve got a run that we haven’t processed yet.
	// figure out the next times that we need to create
	// jobs at (or anything we missed).
	// missedRun 为最近丢失的时间，nextRun 为即将运行的时间
	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		logg.Error(err, "unable to figure out CronJob schedule")
		// we don't really care about requeuing until we get an update that
		// fixes the schedule, so don't return an error
		return ctrl.Result{}, nil
	}

	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())} // save this so we can re-use it elsewhere
	logg = logg.WithValues("now", r.Now(), "next run", nextRun)

	// 6. Run a new job if it’s on schedule, not past the deadline, and not blocked by our concurrency policy
	if missedRun.IsZero() {
		logg.V(1).Info("no upcoming scheduled times, sleeping until next")
		return scheduledResult, nil
	}
	// make sure we're not too late to start the run
	logg = logg.WithValues("current run", missedRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Now())
	}
	// too late sleep
	if tooLate {
		logg.V(1).Info("missed starting deadline for last run, sleeping till next")
		return scheduledResult, nil
	}

	// If we actually have to run a job, we’ll need to either wait till existing ones finish, replace the existing ones, or just add new ones.
	// If our information is out of date due to cache delay, we’ll get a requeue when we get up-to-date information.
	// figure out how to run this job -- concurrency policy might forbid us from running
	// multiple at the same time...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		logg.V(1).Info("concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
		return scheduledResult, nil
	}

	// ...or instruct us to replace existing ones...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			// we don't care if the job was already deleted
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				logg.Error(err, "unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err
			}
		}
	}

	// actually make the job...
	job, err := constructJobForCronJob(&cronJob, missedRun, r)
	if err != nil {
		logg.Error(err, "unable to construct job from template")
		// don't bother requeuing until we get a change to the spec
		return scheduledResult, nil
	}

	// ...and create it on the cluster
	if err := r.Create(ctx, job); err != nil {
		logg.Error(err, "unable to create Job for CronJob", "job", job)
		return ctrl.Result{}, err
	}

	logg.V(1).Info("created Job for CronJob run", "job", job)

	// 7. Requeue when we either see a running job or it’s time for the next scheduled run
	// we'll requeue once we see the running job, and update our status
	return scheduledResult, nil
}

// SetupWithManager sets up the controller with the Manager.
// add this reconciler to the manager
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	//  In order to allow our reconciler to quickly look up Jobs by their owner, we’ll need an index.
	// We declare an index key that we can later use with the client as a pseudo-field name, and then describe how to extract the indexed value from the Job object.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}

		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Complete(r)
}

type realClock struct {
}

func (_ realClock) Now() time.Time {
	return time.Now()
}

// Clock which will allow us to fake timing in our tests
type Clock interface {
	Now() time.Time
}

// isJobFinished Function
// Status conditions allow us to add extensible status information to our objects
// that other humans and controllers can examine to check things like completion and health.
func isJobFinished(job *kbatch.Job) (bool, kbatch.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}

	return false, ""
}

// getScheduledTimeForJob Function
// We’ll use a helper to extract the scheduled time from the annotation that we added during job creation.
// 从创建 job 产生的 annotation 中提取 scheduled time
func getScheduledTimeForJob(job *kbatch.Job) (*time.Time, error) {
	timeRaw := job.Annotations[scheduledTimeAnnotation]
	if len(timeRaw) == 0 {
		return nil, nil
	}

	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return nil, err
	}

	return &timeParsed, nil
}

// getNextSchedule Function
// We’ll calculate the next scheduled time using our helpful cron library.
// we will start calculating appropriate times from our last run, or the creation of the CronJob if we cannot find a last run.
func getNextSchedule(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
	sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("Unparseable schedule %q: %v ", cronJob.Spec.Schedule, err)
	}

	var earliestTime time.Time
	// 上次的调用时间或者是创建时间
	if cronJob.Status.LastScheduleTime != nil {
		earliestTime = cronJob.Status.LastScheduleTime.Time
	} else {
		earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
	}

	if cronJob.Spec.StartingDeadlineSeconds != nil {
		// controller is not going to schedule anything belong this point
		schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))
		if schedulingDeadline.After(earliestTime) {
			earliestTime = schedulingDeadline
		}
	}
	if earliestTime.After(now) {
		return time.Time{}, sched.Next(now), nil
	}

	starts := 0
	// ------------|------------|-------------|
	// earliestTime|--------------------------|now
	for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
		// 最后错过的时间
		lastMissed = t
		starts++
		if starts > 100 {
			// // We can't get the most recent times so just return an empty slice
			return time.Time{}, time.Time{}, fmt.Errorf("too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew")
		}
	}
	return lastMissed, sched.Next(now), nil
}

// constructJobForCronJob Function
// We need to construct a job based on our CronJob’s template. We’ll copy over the spec from the template and copy some basic object meta.
// Then, we’ll set the “scheduled time” annotation so that we can reconstitute our LastScheduleTime field each reconcile.
// Finally, we’ll need to set an owner reference. This allows the Kubernetes garbage collector to clean up jobs when we delete the CronJob,
// and allows controller-runtime to figure out which cronjob needs to be reconciled when a given job changes (is added, deleted, completes, etc).
func constructJobForCronJob(cronJob *batchv1.CronJob, scheduledTime time.Time, r *CronJobReconciler) (*kbatch.Job, error) {
	// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
	name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

	job := &kbatch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
			Name:        name,
			Namespace:   cronJob.Namespace,
		},
		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	}
	for k, v := range cronJob.Spec.JobTemplate.Annotations {
		job.Annotations[k] = v
	}
	job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
	for k, v := range cronJob.Spec.JobTemplate.Labels {
		job.Labels[k] = v
	}
	if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
		return nil, err
	}

	return job, nil
}
