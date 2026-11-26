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
	"errors"
	"testing"

	metal3api "github.com/metal3-io/baremetal-operator/apis/metal3.io/v1alpha1"
	"github.com/metal3-io/baremetal-operator/pkg/provisioner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type liveISOProvisionRecorder struct {
	*mockProvisioner
	called bool
	data   provisioner.ProvisionData
}

func (p *liveISOProvisionRecorder) Provision(_ context.Context, data provisioner.ProvisionData, _ bool) (provisioner.Result, error) {
	p.called = true
	p.data = data
	return provisioner.Result{Dirty: true}, nil
}

func TestProvisioningWithDataImage(t *testing.T) {
	for _, tc := range []struct {
		name          string
		liveISO       bool
		present       bool
		deleting      bool
		readError     bool
		wantProvision bool
		wantURL       string
	}{
		{name: "live ISO forwards configuration image", liveISO: true, present: true, wantProvision: true, wantURL: "https://example.com/configuration.iso"},
		{name: "live ISO without DataImage retains existing behavior", liveISO: true, wantProvision: true},
		{name: "deleting DataImage delays deployment", liveISO: true, present: true, deleting: true},
		{name: "DataImage read failure prevents deployment", liveISO: true, readError: true},
		{name: "ordinary deployment does not read DataImage", present: true, readError: true, wantProvision: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := newDefaultHost(t)
			host.UID = "host-uid"
			host.Spec.Image = &metal3api.Image{URL: "https://example.com/boot.iso"}
			if tc.liveISO {
				host.Spec.Image.DiskFormat = ptr.To("live-iso")
			}
			scheme := runtime.NewScheme()
			require.NoError(t, metal3api.AddToScheme(scheme))
			builder := fakeclient.NewClientBuilder().WithScheme(scheme)
			if tc.present {
				dataImage := &metal3api.DataImage{
					ObjectMeta: metav1.ObjectMeta{Name: host.Name, Namespace: host.Namespace, UID: "image-uid", Generation: 1},
					Spec:       metal3api.DataImageSpec{URL: "https://example.com/configuration.iso"},
				}
				if tc.wantProvision && tc.liveISO {
					dataImage.Finalizers = []string{metal3api.DataImageFinalizer}
					claim := newImageClaim(host, dataImage, *host.Spec.Image)
					require.NoError(t, writeImageClaim(host, liveISODataImageAnnotation, claim))
					require.NoError(t, writeImageClaim(dataImage, deploymentDataImageOwnerAnnotation, claim))
				}
				if tc.deleting {
					now := metav1.Now()
					dataImage.DeletionTimestamp = &now
					dataImage.Finalizers = []string{metal3api.DataImageFinalizer}
				}
				builder = builder.WithObjects(dataImage)
			}
			if tc.readError {
				builder = builder.WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*metal3api.DataImage); ok {
							return errors.New("DataImage API unavailable")
						}
						return c.Get(ctx, key, obj, opts...)
					},
				})
			}
			r := &BareMetalHostReconciler{Client: builder.Build()}
			prov := &liveISOProvisionRecorder{mockProvisioner: newMockProvisioner()}
			info := &reconcileInfo{
				host:    host,
				log:     logf.Log,
				request: ctrl.Request{NamespacedName: types.NamespacedName{Name: host.Name, Namespace: host.Namespace}},
			}
			result := r.actionProvisioning(t.Context(), prov, info)
			assert.Equal(t, tc.wantProvision, prov.called)
			if tc.wantProvision {
				assert.Equal(t, tc.wantURL, prov.data.DataImageURL)
			}
			if tc.readError && tc.liveISO {
				assert.IsType(t, actionError{}, result)
			}
			if tc.deleting {
				assert.IsType(t, actionContinue{}, result)
			}
		})
	}
}

func TestLiveISOClaimAndInputChanges(t *testing.T) {
	host := newDefaultHost(t)
	host.UID = "host-uid"
	host.Spec.Image = &metal3api.Image{URL: "https://example.com/live.iso", DiskFormat: ptr.To("live-iso")}
	image := &metal3api.DataImage{ObjectMeta: metav1.ObjectMeta{Name: host.Name, Namespace: host.Namespace, UID: "image-uid", Generation: 1}, Spec: metal3api.DataImageSpec{URL: "https://example.com/config.iso"}}
	scheme := runtime.NewScheme()
	require.NoError(t, metal3api.AddToScheme(scheme))
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&metal3api.DataImage{}).WithObjects(host, image).Build()
	r := &BareMetalHostReconciler{Client: c}
	info := &reconcileInfo{host: host, log: logf.Log, request: ctrl.Request{NamespacedName: client.ObjectKeyFromObject(host)}}
	url, result := r.liveISODataImage(t.Context(), info, *host.Spec.Image)
	require.Empty(t, url)
	require.IsType(t, actionContinue{}, result)
	require.False(t, hasLiveISODataImage(host))
	_, result = r.liveISODataImage(t.Context(), info, *host.Spec.Image)
	require.IsType(t, actionContinue{}, result)
	claim, err := readLiveISODataImage(host)
	require.NoError(t, err)
	require.Equal(t, image.UID, claim.UID)
	// Simulate a fresh reconcile after the host's claim was persisted.
	host = host.DeepCopy()
	info.host = host
	_, result = r.liveISODataImage(t.Context(), info, *host.Spec.Image)
	require.IsType(t, actionContinue{}, result)
	url, result = r.liveISODataImage(t.Context(), info, *host.Spec.Image)
	require.Nil(t, result)
	require.Equal(t, image.Spec.URL, url)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(image), image))
	image.Spec.URL = "https://example.com/replaced.iso"
	image.Generation++
	require.NoError(t, c.Update(t.Context(), image))
	url, result = r.liveISODataImage(t.Context(), info, *host.Spec.Image)
	require.Empty(t, url)
	require.IsType(t, actionFailed{}, result)
	require.NoError(t, c.Delete(t.Context(), image))
	_, result = r.liveISODataImage(t.Context(), info, *host.Spec.Image)
	require.IsType(t, actionContinue{}, result)
	// Simulate forced disappearance: a restart must not make it optional.
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(image), image))
	image.Finalizers = nil
	require.NoError(t, c.Update(t.Context(), image))
	_, result = r.liveISODataImage(t.Context(), info, *host.Spec.Image)
	require.IsType(t, actionError{}, result)
}

type liveISOStatusProvisioner struct {
	*mockProvisioner
	attached     bool
	err          error
	legacyCalled bool
}

func (p *liveISOStatusProvisioner) GetDataImageStatus(context.Context) (bool, error) {
	p.legacyCalled = true
	return true, nil
}
func (p *liveISOStatusProvisioner) GetConfigDriveStatus(context.Context, string) (bool, error) {
	return p.attached, p.err
}

type liveISOFactory struct{ prov provisioner.Provisioner }

func (f liveISOFactory) NewProvisioner(context.Context, provisioner.HostData, provisioner.EventPublisher) (provisioner.Provisioner, error) {
	return f.prov, nil
}

func TestDeploymentDataImageStatusAndDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, metal3api.AddToScheme(scheme))
	for _, test := range []struct {
		name                         string
		attached, deleting, released bool
		readError                    error
	}{
		{name: "configuration attached", attached: true},
		{name: "boot image alone is insufficient"},
		{name: "read error preserves ownership", readError: errors.New("BMC unavailable")},
		{name: "deletion waits for deployment teardown", deleting: true},
		{name: "teardown releases finalizer despite unrelated boot image", deleting: true, released: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := newDefaultHost(t)
			host.UID = "host-uid"
			image := &metal3api.DataImage{ObjectMeta: metav1.ObjectMeta{Name: host.Name, Namespace: host.Namespace, UID: "image-uid", Generation: 1, Finalizers: []string{metal3api.DataImageFinalizer}, OwnerReferences: []metav1.OwnerReference{{UID: host.UID}}}, Spec: metal3api.DataImageSpec{URL: "https://example.com/config.iso"}}
			if test.deleting {
				now := metav1.Now()
				image.DeletionTimestamp = &now
			}
			host.Spec.Image = &metal3api.Image{URL: "https://example.com/live.iso", DiskFormat: ptr.To("live-iso")}
			claim := newImageClaim(host, image, *host.Spec.Image)
			if !test.released {
				require.NoError(t, writeImageClaim(host, liveISODataImageAnnotation, claim))
			}
			claim.Released = test.released
			require.NoError(t, writeImageClaim(image, deploymentDataImageOwnerAnnotation, claim))
			c := fakeclient.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&metal3api.DataImage{}).WithObjects(host, image).Build()
			prov := &liveISOStatusProvisioner{mockProvisioner: newMockProvisioner(), attached: test.attached, err: test.readError}
			r := &DataImageReconciler{Client: c, Log: logf.Log, ProvisionerFactory: liveISOFactory{prov: prov}}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(image)})
			if test.readError != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.False(t, prov.legacyCalled, "deployment-owned images must never use any-image observation")
			err = c.Get(t.Context(), client.ObjectKeyFromObject(image), image)
			if test.released {
				require.True(t, client.IgnoreNotFound(err) == nil)
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Contains(t, image.Finalizers, metal3api.DataImageFinalizer)
			if test.attached {
				require.Equal(t, image.Spec.URL, image.Status.AttachedImage.URL)
			} else {
				require.Empty(t, image.Status.AttachedImage.URL)
			}
		})
	}
}

func TestMalformedOrChangedLiveISOOwnershipFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*metal3api.BareMetalHost)
	}{
		{name: "malformed ownership", change: func(h *metal3api.BareMetalHost) { h.Annotations[liveISODataImageAnnotation] = "{" }},
		{name: "changed BMC target", change: func(h *metal3api.BareMetalHost) {
			h.Spec.BMC.Address = "redfish-virtualmedia+https://other.example.com/redfish/v1/Systems/1"
		}},
		{name: "changed physical host UID", change: func(h *metal3api.BareMetalHost) { h.UID = "new-host-uid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := newDefaultHost(t)
			host.UID = "host-uid"
			boot := metal3api.Image{URL: "https://example.com/live.iso", DiskFormat: ptr.To("live-iso")}
			image := &metal3api.DataImage{ObjectMeta: metav1.ObjectMeta{UID: "image-uid", Generation: 1}, Spec: metal3api.DataImageSpec{URL: "https://example.com/config.iso"}}
			require.NoError(t, writeImageClaim(host, liveISODataImageAnnotation, newImageClaim(host, image, boot)))
			tc.change(host)
			r := &BareMetalHostReconciler{}
			// A nil client proves malformed identity is rejected before even
			// fetching inputs, much less calling a provisioner or BMC.
			_, result := r.liveISODataImage(t.Context(), &reconcileInfo{host: host}, boot)
			require.IsType(t, actionError{}, result)
		})
	}
}

func TestLiveISOTeardownAcknowledgementSurvivesInterruption(t *testing.T) {
	host := newDefaultHost(t)
	host.UID = "host-uid"
	boot := metal3api.Image{URL: "https://example.com/live.iso", DiskFormat: ptr.To("live-iso")}
	host.Status.Provisioning.Image = boot
	image := &metal3api.DataImage{ObjectMeta: metav1.ObjectMeta{Name: host.Name, Namespace: host.Namespace, UID: "image-uid", Generation: 1, Finalizers: []string{metal3api.DataImageFinalizer}}, Spec: metal3api.DataImageSpec{URL: "https://example.com/config.iso"}}
	require.NoError(t, writeImageClaim(host, liveISODataImageAnnotation, newImageClaim(host, image, boot)))
	scheme := runtime.NewScheme()
	require.NoError(t, metal3api.AddToScheme(scheme))
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(host, image).Build()
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(host), host))
	info := &reconcileInfo{host: host, request: ctrl.Request{NamespacedName: client.ObjectKeyFromObject(host)}}
	r := &BareMetalHostReconciler{Client: c}
	// The provisioner has completed teardown, but the DataImage reconciler
	// never observed the claim before this cancellation.
	require.IsType(t, actionContinue{}, r.releaseLiveISODataImage(t.Context(), info))
	require.True(t, hasLiveISODataImage(host))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(image), image))
	owner, err := readImageClaim(image, deploymentDataImageOwnerAnnotation)
	require.NoError(t, err)
	require.True(t, owner.Released)
	// Recreate the reconciler and host object to simulate a manager restart.
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(host), host))
	r = &BareMetalHostReconciler{Client: c}
	require.IsType(t, actionContinue{}, r.releaseLiveISODataImage(t.Context(), info))
	require.False(t, hasLiveISODataImage(host))
}
