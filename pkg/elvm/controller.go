package elvm

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/lvmd/commands"
	"github.com/google/lvmd/parser"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type elvmControllerServer struct {
	volumeGroup *parser.VG
}

func (server *elvmControllerServer) ControllerExpandVolume(ctx context.Context, request *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	log.Println("CONTROLLER: EXPAND_VOLUME")
	return nil, nil
}

func (server *elvmControllerServer) ControllerGetVolume(ctx context.Context, reqeust *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	log.Println("CONTROLLER: GET_VOLUME")
	return nil, nil
}

func (server *elvmControllerServer) CreateSnapshot(ctx context.Context, reqeust *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	log.Println("CONTROLLER: CREATE_SNAPSHOT")
	return nil, nil
}

func (server *elvmControllerServer) DeleteSnapshot(ctx context.Context, request *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	log.Println("CONTROLLER: DELETE_SNAPSHOT")
	return nil, nil
}

func (server *elvmControllerServer) ListSnapshots(ctx context.Context, reqeust *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	log.Println("CONTROLLER: LIST_SNAPSHOTS")
	return nil, nil
}

func (server *elvmControllerServer) CreateVolume(ctx context.Context, request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// Make sure that we have a name
	if len(request.Name) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] ControllerCreateVolume Name must be provided.",
		)
	}

	// Make sure that we have been given the capability that we require
	if len(request.VolumeCapabilities) != 1 || request.VolumeCapabilities[0].AccessMode.Mode != SUPPORTED_CAPABILITY {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] ControllerCreateVolume Only `multi_node_multi_writer` is allowed as a volume capability.",
		)
	}

	// Make sure that the size is valid
	if request.CapacityRange == nil || request.CapacityRange.RequiredBytes < 0 || request.CapacityRange.LimitBytes < 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] ControllerCreateVolume CapacityRange must be provided.",
		)
	}

	vg, err := getCurrentVG(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] ControllerCreateVolume Could not get selected volume group: %s", err.Error()),
		)
	}

	available := vg.FreeSize
	if request.CapacityRange.RequiredBytes < 0 || uint64(request.CapacityRange.RequiredBytes) > available {
		return nil, status.Error(
			codes.ResourceExhausted,
			fmt.Sprintf(
				"[ERROR] ControllerCreateVolume Not enough space available for request. Requested (%d) > Available (%d)",
				request.CapacityRange.RequiredBytes,
				available,
			),
		)
	}

	capacity, err := getCapacity(available, uint64(request.CapacityRange.RequiredBytes), uint64(request.CapacityRange.LimitBytes))
	if err != nil {
		return nil, status.Error(
			codes.FailedPrecondition,
			fmt.Sprintf("[ERROR] ControllerCreateVolume Requested capacity cannot be allocated: %s", err.Error()),
		)
	}

	// Generate a unique name with the following format: elvm-csi-HASH
	// Note: HASH is the fnv hash of the name
	hasher := fnv.New64a()
	hasher.Write([]byte(request.Name))
	volumeName := fmt.Sprintf("elvm-csi-%d", hasher.Sum64())

	// Make sure that this volume does not exist already
	lvs, err := getCurrentLVs(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] ControllerCreateVolume Could not list logical volumes: %s", err.Error()),
		)
	}

	for _, lv := range lvs {
		if lv.Name == volumeName {
			return nil, status.Error(
				codes.AlreadyExists,
				fmt.Sprintf("[ERROR] ControllerCreateVolume Volume exists already: %s", lv),
			)
		}
	}

	// Create some unique tags to show ownership
	tags := []string{
		ELVM_TAG,
		fmt.Sprintf("ELVM_NAME_%s", request.Name),
	}

	// Actually create the volume
	createOut, createErr := commands.CreateLV(ctx, server.volumeGroup.Name, volumeName, capacity, 0, tags)
	if createErr != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] ControllerCreateVolume Could not create logical volume: %s | %s",
				createOut,
				createErr.Error(),
			),
		)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			CapacityBytes: int64(capacity),
			VolumeId: volumeName,
		},
	}, nil
}

func (server *elvmControllerServer) DeleteVolume(ctx context.Context, request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// Make sure that we have a valid ID to delete
	if len(request.VolumeId) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] ControllerDeleteVolume Volume ID must be provided.",
		)
	}

	// Make sure that the volume group still exists
	_, err := getCurrentVG(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] ControllerCreateVolume Could not get selected volume group: %s", err.Error()),
		)
	}

	// Get all of the logical volumes
	lvs, err := getCurrentLVs(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] ControllerDeleteVolume Could not list logical volumes: %s", err.Error()),
		)
	}

	// Make sure that the requested volume exists
	var selectedLogicalVolume *parser.LV
	for _, lv := range lvs {
		if lv.Name == request.VolumeId {
			selectedLogicalVolume = lv
			break
		}
	}

	if selectedLogicalVolume == nil {
		return nil, status.Error(
			codes.NotFound,
			fmt.Sprintf("[ERROR] ControllerDeleteVolume Could not find requested logical volume: %s", request.VolumeId),
		)
	}

	// Make sure that the volume is managed by ELVM
	hasELVMTag := false
	for _, tag := range selectedLogicalVolume.Tags {
		if tag == ELVM_TAG {
			hasELVMTag = true
			break
		}
	}

	if !hasELVMTag {
		return nil, status.Error(
			codes.Aborted,
			"[ERROR] ContollerDeleteVolume Found volume to delete but it is not managed by ELVM. Aborting.",
		)
	}

	// Actually delete the logical volume
	output, err := commands.RemoveLV(ctx, server.volumeGroup.Name, selectedLogicalVolume.Name)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] ControllerDeleteVolume Could not delete logical volume: %s | %s",
				output,
				err.Error(),
			),
		)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (server *elvmControllerServer) ControllerPublishVolume(ctx context.Context, request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Println("CONTROLLER: PUBLISH_VOLUME")
	return nil, nil
}

func (server *elvmControllerServer) ControllerUnpublishVolume(ctx context.Context, request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Println("CONTROLLER: UNPUBLISH_VOLUME")
	return nil, nil
}

func (server *elvmControllerServer) ValidateVolumeCapabilities(ctx context.Context, request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	log.Println("CONTROLLER: VALIDATE_VOLUME_CAPABILITIES")
	return nil, nil
}

func (server *elvmControllerServer) ListVolumes(ctx context.Context, request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	log.Println("CONTROLLER: LIST_VOLUMES")
	return nil, nil
}

func (server *elvmControllerServer) GetCapacity(ctx context.Context, request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	log.Println("CONTROLLER: GET_CAPACITY")
	return nil, nil
}

func (server *elvmControllerServer) ControllerGetCapabilities(ctx context.Context, request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	// Helper method to create a nested struct needed for the capabilities
	toCapability := func (capability csi.ControllerServiceCapability_RPC_Type) (*csi.ControllerServiceCapability) {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: capability,
				},
			},
		}
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			toCapability(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
			toCapability(csi.ControllerServiceCapability_RPC_LIST_VOLUMES),
			toCapability(csi.ControllerServiceCapability_RPC_GET_CAPACITY),
		},
	}, nil
}
