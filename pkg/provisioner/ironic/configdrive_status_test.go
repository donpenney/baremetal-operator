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

package ironic

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack/baremetal/v1/nodes"
	"github.com/metal3-io/baremetal-operator/pkg/provisioner/ironic/clients"
	"github.com/stretchr/testify/require"
)

func TestConfigDriveIdentity(t *testing.T) {
	fingerprint := func(value string) string {
		digest := sha256.Sum256([]byte(value))
		return hex.EncodeToString(digest[:])
	}
	const source = "https://example.com/attempt/config.iso"
	for _, tc := range []struct {
		name, body, recordSource, reservation         string
		code, version                                 int
		attached, wantError, noRecord                 bool
		noSourceFingerprint, noPublicationFingerprint bool
	}{
		{name: "configuration present with redacted URLs", body: `[{"image":"cached-config.iso","inserted":true}]`, attached: true},
		{name: "boot image alone", body: `[{"image":"boot.iso","inserted":true}]`},
		{name: "configuration not inserted", body: `[{"image":"cached-config.iso","inserted":false}]`},
		{name: "other media alongside configuration", body: `[{"image":"boot.iso","inserted":true},{"image":"cached-config.iso","inserted":true},{"image":"unrelated.iso","inserted":true}]`, attached: true},
		{name: "inserted media without a URL", body: `[{"inserted":true}]`},
		{name: "source fingerprint unavailable", noSourceFingerprint: true, wantError: true},
		{name: "publication pending", noPublicationFingerprint: true},
		{name: "source from another attempt", recordSource: "https://example.com/old/config.iso", wantError: true},
		{name: "busy node", reservation: "conductor", wantError: true},
		{name: "API failure", code: 503, wantError: true},
		{name: "malformed response", body: `invalid`, wantError: true},
		{name: "lost deployment identity", noRecord: true, wantError: true},
		{name: "unsupported observation", version: 89, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				require.Equal(t, "/v1/nodes/node-id/vmedia", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				if tc.code != 0 {
					w.WriteHeader(tc.code)
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			recordSource := tc.recordSource
			if recordSource == "" {
				recordSource = source
			}
			node := &nodes.Node{UUID: "node-id", ProvisionState: string(nodes.Active), Reservation: tc.reservation, DriverInternalInfo: map[string]any{"ramdisk_virtual_media": map[string]any{"configdrive_source": "******", "configdrive": "******", "configuration_source_sha256": fingerprint(recordSource), "configuration_media_sha256": fingerprint("cached-config.iso")}}}
			if tc.noSourceFingerprint {
				delete(node.DriverInternalInfo["ramdisk_virtual_media"].(map[string]any), "configuration_source_sha256")
			}
			if tc.noPublicationFingerprint {
				delete(node.DriverInternalInfo["ramdisk_virtual_media"].(map[string]any), "configuration_media_sha256")
			}
			if tc.noRecord {
				node.DriverInternalInfo = nil
			}
			version := tc.version
			if version == 0 {
				version = 95
			}
			p := &ironicProvisioner{nodeID: "node-id", cachedNode: node, availableFeatures: clients.AvailableFeatures{MaxVersion: version}, client: &gophercloud.ServiceClient{ProviderClient: &gophercloud.ProviderClient{HTTPClient: *server.Client()}, Endpoint: server.URL + "/v1/"}}
			attached, err := p.GetConfigDriveStatus(t.Context(), source)
			require.Equal(t, tc.attached, attached)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.reservation != "" || tc.recordSource != "" || tc.noRecord || tc.noSourceFingerprint || tc.noPublicationFingerprint || version < 93 {
				require.Zero(t, requests)
			}
		})
	}
}
