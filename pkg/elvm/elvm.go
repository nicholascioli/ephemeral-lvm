package elvm

import (
	"context"
	"fmt"
	"log"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/lvmd/commands"
	"github.com/google/lvmd/parser"
)

type ELVM struct {
	//
}

type ELVMArgs struct {
	FsType string
	NodeId string
	VolumeGroup string
}

const (
	ELVM_TAG = "ELVM_CSI_VOLUME"
	SUPPORTED_CAPABILITY = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
)

func (server ELVM) GetCSIEndpoints(args *ELVMArgs) (*elvmIdentityServer, *elvmControllerServer, *elvmNodeServer) {
	// Get all of the available volume groups
	volumeGroups, err := commands.ListVG(context.Background())
	if err != nil {
		log.Fatalln("[ERROR] Could not list volume groups:", err.Error())
	}

	// Ensure that the supplied volume group is available
	var selectedVolumeGroup *parser.VG
	for _, vg := range volumeGroups {
		if vg.Name == args.VolumeGroup {
			selectedVolumeGroup = vg
			break
		}
	}

	if selectedVolumeGroup == nil {
		log.Fatalln(fmt.Sprintf("[ERROR] Could not find volume group '%s' in %s", args.VolumeGroup, volumeGroups))
	}

	// Make sure that the default fs type is available
	if !isCommandAvailable("mkfs." + args.FsType) {
		log.Fatalln(fmt.Sprintf("[ERROR] Could not find default fs executable: mkfs.%s", args.FsType))
	}

	// Return the actual implementations
	return &elvmIdentityServer{
		volumeGroup: selectedVolumeGroup,
	}, &elvmControllerServer{
		volumeGroup: selectedVolumeGroup,
	}, &elvmNodeServer{
		volumeGroup: selectedVolumeGroup,
		nodeId: args.NodeId,
		fsType: args.FsType,
	}
}
