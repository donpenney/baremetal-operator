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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/gophercloud/gophercloud/v2/openstack/baremetal/v1/nodes"
	"github.com/metal3-io/baremetal-operator/pkg/provisioner"
)

func (p *ironicProvisioner) GetConfigDriveStatus(ctx context.Context, sourceURL string) (bool, error) {
	if sourceURL == "" {
		return false, fmt.Errorf("configuration source URL must not be empty")
	}
	if !p.availableFeatures.HasVirtualMediaGetAPI() {
		return false, fmt.Errorf("ironic virtual-media observation requires API version 1.93")
	}
	node, err := p.getNode(ctx)
	if err != nil {
		return false, err
	}
	if node.Reservation != "" {
		return false, provisioner.ErrNodeIsBusy
	}
	media, ok := node.DriverInternalInfo["ramdisk_virtual_media"].(map[string]any)
	if !ok {
		if nodes.ProvisionState(node.ProvisionState) == nodes.Active {
			return false, fmt.Errorf("active live ISO deployment has no recorded configuration identity")
		}
		return false, nil
	}
	// Ironic redacts configdrive URLs in API responses. Compare URL hashes
	// rather than requiring permission to read the underlying secrets.
	sourceHash := sha256.Sum256([]byte(sourceURL))
	if media["configuration_source_sha256"] != hex.EncodeToString(sourceHash[:]) {
		return false, fmt.Errorf("ironic deployment configuration does not match the claimed DataImage")
	}
	publishedHash, ok := media["configuration_media_sha256"].(string)
	if !ok || publishedHash == "" {
		return false, nil
	}
	result := nodes.GetVirtualMedia(ctx, p.client, p.nodeID)
	if result.Err != nil {
		return false, result.Err
	}
	var devices []VirtualMedia
	if err := result.ExtractInto(&devices); err != nil {
		return false, err
	}
	for _, device := range devices {
		if device.Inserted {
			imageHash := sha256.Sum256([]byte(device.Image))
			if hex.EncodeToString(imageHash[:]) == publishedHash {
				return true, nil
			}
		}
	}
	return false, nil
}
