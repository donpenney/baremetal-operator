/*
Copyright 2026 Red Hat, Inc.

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
	"encoding/json"
	"errors"
	"fmt"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	"github.com/metal3-io/baremetal-operator/pkg/provisioner"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const liveISODataImageAnnotation = "baremetalhost.metal3.io/live-iso-dataimage"
const deploymentDataImageOwnerAnnotation = "dataimage.metal3.io/deployment-owner"

// These annotations are controller-owned durable state, not request inputs.
// Released is written only on the DataImage after provisioner teardown succeeds.
// A missing host annotation alone is never proof that media was released.
type liveISODataImageClaim struct {
	Version        int       `json:"version"`
	HostUID        types.UID `json:"hostUID"`
	UID            types.UID `json:"uid"`
	Generation     int64     `json:"generation"`
	URL            string    `json:"url"`
	BootURL        string    `json:"bootURL"`
	BMCAddress     string    `json:"bmcAddress"`
	BootMACAddress string    `json:"bootMACAddress"`
	Released       bool      `json:"released,omitempty"`
}

func hasLiveISODataImage(host *metal3api.BareMetalHost) bool {
	_, present := host.Annotations[liveISODataImageAnnotation]
	return present
}

func readImageClaim(object client.Object, key string) (*liveISODataImageClaim, error) {
	raw, present := object.GetAnnotations()[key]
	if !present {
		return nil, nil
	}
	var claim liveISODataImageClaim
	if err := json.Unmarshal([]byte(raw), &claim); err != nil {
		return nil, fmt.Errorf("invalid live ISO media ownership annotation: %w", err)
	}
	if claim.Version != 1 || claim.UID == "" || claim.HostUID == "" || claim.Generation < 1 || claim.URL == "" || claim.BootURL == "" {
		return nil, fmt.Errorf("incomplete or unsupported live ISO media ownership annotation")
	}
	return &claim, nil
}
func readLiveISODataImage(host *metal3api.BareMetalHost) (*liveISODataImageClaim, error) {
	claim, err := readImageClaim(host, liveISODataImageAnnotation)
	if err != nil || claim == nil {
		return claim, err
	}
	if claim.HostUID != host.UID || claim.BMCAddress != host.Spec.BMC.Address || claim.BootMACAddress != host.Spec.BootMACAddress || claim.Released {
		return nil, fmt.Errorf("live ISO deployment host identity changed")
	}
	return claim, nil
}
func writeImageClaim(object client.Object, key string, claim *liveISODataImageClaim) error {
	content, err := json.Marshal(claim)
	if err != nil {
		return err
	}
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[key] = string(content)
	object.SetAnnotations(annotations)
	return nil
}
func newImageClaim(host *metal3api.BareMetalHost, image *metal3api.DataImage, boot metal3api.Image) *liveISODataImageClaim {
	return &liveISODataImageClaim{Version: 1, HostUID: host.UID, UID: image.UID, Generation: image.Generation, URL: image.Spec.URL, BootURL: boot.URL, BMCAddress: host.Spec.BMC.Address, BootMACAddress: host.Spec.BootMACAddress}
}

// Claim input durably before making a provisioner request. A retry cannot
// silently boot without configuration if its DataImage changes or disappears.
func (r *BareMetalHostReconciler) liveISODataImage(ctx context.Context, info *reconcileInfo, image metal3api.Image) (string, actionResult) {
	reference, err := readLiveISODataImage(info.host)
	if err != nil {
		return "", actionError{err}
	}
	if !image.IsLiveISO() {
		if reference != nil {
			return "", recordActionFailure(info, metal3api.ProvisioningError, "live ISO deployment must be torn down before changing its image")
		}
		return "", nil
	}
	dataImage := &metal3api.DataImage{}
	if err := r.Get(ctx, info.request.NamespacedName, dataImage); err != nil {
		if k8serrors.IsNotFound(err) && reference == nil {
			return "", nil
		}
		return "", actionError{fmt.Errorf("could not load required live ISO DataImage: %w", err)}
	}
	if !dataImage.DeletionTimestamp.IsZero() {
		return "", actionContinue{dataImageUpdateDelay}
	}
	if dataImage.Spec.URL == "" {
		return "", recordActionFailure(info, metal3api.ProvisioningError, "live ISO DataImage URL must not be empty")
	}
	owner, err := readImageClaim(dataImage, deploymentDataImageOwnerAnnotation)
	if err != nil {
		return "", actionError{err}
	}
	desired := newImageClaim(info.host, dataImage, image)
	if reference == nil {
		if owner != nil && (*owner != *desired || owner.Released) {
			return "", recordActionFailure(info, metal3api.ProvisioningError, "previous deployment DataImage must be deleted before a new deployment")
		}
		if controllerutil.AddFinalizer(dataImage, metal3api.DataImageFinalizer) {
			if err := r.Update(ctx, dataImage); err != nil {
				return "", actionError{fmt.Errorf("could not protect live ISO DataImage: %w", err)}
			}
			return "", actionContinue{}
		}
		if err := writeImageClaim(info.host, liveISODataImageAnnotation, desired); err != nil {
			return "", actionError{err}
		}
		if err := r.Update(ctx, info.host); err != nil {
			return "", actionError{fmt.Errorf("could not persist live ISO deployment identity: %w", err)}
		}
		return "", actionContinue{}
	}
	if *reference != *desired {
		return "", recordActionFailure(info, metal3api.ProvisioningError, "live ISO deployment input changed; stop the deployment before replacing its configuration")
	}
	if owner != nil && *owner != *reference {
		return "", actionError{fmt.Errorf("live ISO DataImage belongs to a different or released deployment")}
	}
	if owner == nil {
		if err := writeImageClaim(dataImage, deploymentDataImageOwnerAnnotation, reference); err != nil {
			return "", actionError{err}
		}
		if err := r.Update(ctx, dataImage); err != nil {
			return "", actionError{fmt.Errorf("could not record live ISO DataImage ownership: %w", err)}
		}
		return "", actionContinue{}
	}
	return reference.URL, nil
}

func (r *DataImageReconciler) finishReleasedDataImage(ctx context.Context, image *metal3api.DataImage) (ctrl.Result, error) {
	if !image.DeletionTimestamp.IsZero() {
		if controllerutil.RemoveFinalizer(image, metal3api.DataImageFinalizer) {
			return ctrl.Result{}, r.Update(ctx, image)
		}
		return ctrl.Result{}, nil
	}
	image.Status.AttachedImage.URL = ""
	image.Status.Error.Message = "deployment released; delete this DataImage before a new deployment"
	return ctrl.Result{RequeueAfter: dataImageUpdateDelay}, r.Status().Update(ctx, image)
}

func (r *DataImageReconciler) reconcileDeploymentDataImage(ctx context.Context, info *rdiInfo) (ctrl.Result, error) {
	image, host := info.di, info.bmh
	reference, err := readLiveISODataImage(host)
	wait := ctrl.Result{RequeueAfter: dataImageUpdateDelay}
	if err != nil {
		return wait, err
	}
	owner, err := readImageClaim(image, deploymentDataImageOwnerAnnotation)
	if err != nil {
		return wait, err
	}
	if owner != nil && owner.UID != image.UID {
		return wait, fmt.Errorf("DataImage ownership identity does not match its resource UID")
	}
	if owner != nil && owner.Released {
		return r.finishReleasedDataImage(ctx, image)
	}
	if reference == nil {
		return wait, fmt.Errorf("live ISO deployment ownership is missing from its host; media release is not acknowledged")
	}
	if reference.UID != image.UID {
		return wait, fmt.Errorf("a different DataImage is still owned by the live ISO deployment")
	}
	if owner != nil && *owner != *reference {
		return wait, fmt.Errorf("DataImage and host deployment identities disagree")
	}
	if owner == nil {
		if err := writeImageClaim(image, deploymentDataImageOwnerAnnotation, reference); err != nil {
			return wait, err
		}
		if err := r.Update(ctx, image); err != nil {
			return wait, err
		}
	}
	image.Status.LastReconciled = &metav1.Time{Time: metav1.Now().Time}
	if !image.DeletionTimestamp.IsZero() {
		image.Status.Error.Message = "waiting for live ISO deployment teardown before deleting its DataImage"
		return wait, r.Status().Update(ctx, image)
	}
	if !ownerReferenceExists(host, image) {
		if err := controllerutil.SetOwnerReference(host, image, r.Scheme()); err != nil {
			return wait, err
		}
		if err := r.Update(ctx, image); err != nil {
			return wait, err
		}
	}
	prov, err := r.ProvisionerFactory.NewProvisioner(ctx, provisioner.BuildHostDataNoBMC(*host), info.publishEvent)
	if err != nil {
		if errors.Is(err, provisioner.ErrNotReady) {
			return wait, nil
		}
		return wait, err
	}
	observer, supported := prov.(provisioner.ConfigDriveProvisioner)
	if !supported {
		return wait, fmt.Errorf("provisioner does not support live ISO configuration observation")
	}
	attached, err := observer.GetConfigDriveStatus(ctx, reference.URL)
	if errors.Is(err, provisioner.ErrNodeIsBusy) {
		return wait, nil
	}
	if err != nil {
		image.Status.Error.Message = err.Error()
		image.Status.Error.Count++
	} else {
		image.Status.Error.Message = ""
		image.Status.Error.Count = 0
		image.Status.AttachedImage.URL = ""
		if attached {
			image.Status.AttachedImage.URL = reference.URL
		}
		if image.Generation != reference.Generation || image.Spec.URL != reference.URL {
			image.Status.Error.Message = "deployment configuration is immutable; stop the deployment before replacing it"
		}
	}
	if updateErr := r.Status().Update(ctx, image); updateErr != nil {
		return wait, updateErr
	}
	return wait, err
}

// Called only after provisioner teardown succeeds. Persist a positive release
// acknowledgement before removing the host claim; absence alone is not proof.
func (r *BareMetalHostReconciler) releaseLiveISODataImage(ctx context.Context, info *reconcileInfo) actionResult {
	reference, err := readLiveISODataImage(info.host)
	if err != nil {
		return actionError{err}
	}
	if reference == nil && !info.host.Status.Provisioning.Image.IsLiveISO() &&
		(info.host.Spec.Image == nil || !info.host.Spec.Image.IsLiveISO()) {
		return nil
	}
	image := &metal3api.DataImage{}
	if err := r.Get(ctx, info.request.NamespacedName, image); err != nil {
		if k8serrors.IsNotFound(err) {
			return r.clearLiveISODataImage(ctx, info)
		}
		return actionError{fmt.Errorf("could not acknowledge deployment media release: %w", err)}
	}
	owner, err := readImageClaim(image, deploymentDataImageOwnerAnnotation)
	if err != nil {
		return actionError{err}
	}
	if reference == nil {
		if owner == nil || owner.Released {
			return nil
		}
		if owner.HostUID != info.host.UID || owner.BMCAddress != info.host.Spec.BMC.Address || owner.BootMACAddress != info.host.Spec.BootMACAddress {
			return actionError{fmt.Errorf("deployment DataImage has a different host owner")}
		}
		reference = owner
	}
	if image.UID != reference.UID {
		return r.clearLiveISODataImage(ctx, info)
	}
	if owner != nil {
		active := *owner
		active.Released = false
		if active != *reference {
			return actionError{fmt.Errorf("deployment DataImage release identity does not match the host")}
		}
	}
	if owner == nil || !owner.Released {
		released := *reference
		released.Released = true
		if err := writeImageClaim(image, deploymentDataImageOwnerAnnotation, &released); err != nil {
			return actionError{err}
		}
		if err := r.Update(ctx, image); err != nil {
			return actionError{fmt.Errorf("could not acknowledge deployment media release: %w", err)}
		}
		return actionContinue{}
	}
	return r.clearLiveISODataImage(ctx, info)
}
func (r *BareMetalHostReconciler) clearLiveISODataImage(ctx context.Context, info *reconcileInfo) actionResult {
	if !hasLiveISODataImage(info.host) {
		return nil
	}
	delete(info.host.Annotations, liveISODataImageAnnotation)
	if err := r.Update(ctx, info.host); err != nil {
		return actionError{err}
	}
	return actionContinue{}
}
