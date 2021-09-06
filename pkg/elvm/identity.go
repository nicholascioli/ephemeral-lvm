package elvm

import (
	"context"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/lvmd/parser"
)

type elvmIdentityServer struct {
	volumeGroup *parser.VG
}

// GetPluginInfo returns metadata of the plugin
func (d *elvmIdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "io.github.csi.elvm",
		VendorVersion: "0.0.1",
	}, nil
}

// GetPluginCapabilities returns available capabilities of the plugin
func (d *elvmIdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

// Probe returns the health and readiness of the plugin
func (d *elvmIdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
