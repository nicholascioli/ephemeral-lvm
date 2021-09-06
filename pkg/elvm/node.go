package elvm

import (
	"context"
	"fmt"
	"log"
	"os"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/lvmd/parser"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type elvmNodeServer struct {
	volumeGroup *parser.VG
	nodeId string
	fsType string
}

func (server *elvmNodeServer) NodeExpandVolume(ctx context.Context, request *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	log.Println("NODE: EXPAND_VOLUME")
	return nil, nil
}

func (server *elvmNodeServer) NodeGetCapabilities(ctx context.Context, reqeust *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// Helper method for capabilities
	toCapability := func (capability csi.NodeServiceCapability_RPC_Type) *csi.NodeServiceCapability {
		return &csi.NodeServiceCapability {
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: capability,
				},
			},
		}
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			toCapability(csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME),
		},
	}, nil
}

func (server *elvmNodeServer) NodeGetInfo(ctx context.Context, request *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: server.nodeId,
	}, nil
}

func (server *elvmNodeServer) NodeGetVolumeStats(ctx context.Context, request *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	log.Println("NODE: GET_VOLUME_STATS")
	return nil, nil
}

func (server *elvmNodeServer) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Make sure that we have a volume ID
	if len(request.VolumeId) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodePublishVolume Volume ID must be provided.",
		)
	}

	// Make sure that we have a staging path
	if len(request.StagingTargetPath) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodePublishVolume Staging target path must be provided.",
		)
	}

	// Make sure that we have a target path
	if len(request.TargetPath) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodePublishVolume Target path must be provided.",
		)
	}

	// Make sure that we have been given the capability that we require
	if request.VolumeCapability == nil || request.VolumeCapability.AccessMode.Mode != SUPPORTED_CAPABILITY {
		return nil, status.Error(
			codes.FailedPrecondition,
			"[ERROR] NodePublishVolume Only `multi_node_multi_writer` is allowed as a volume capability.",
		)
	}

	// Get all of the logical volumes
	lvs, err := getCurrentLVs(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] NodePublishVolume Could not list logical volumes: %s", err.Error()),
		)
	}

	// Make sure that we have the requested volume
	var logicalVolume *parser.LV
	for _, lv := range lvs {
		if lv.Name == request.VolumeId {
			logicalVolume = lv
			break
		}
	}

	if logicalVolume == nil {
		return nil, status.Error(
			codes.NotFound,
			fmt.Sprintf("[ERROR] NodePublishVolume Could not find requested logical volume: %s", request.VolumeId),
		)
	}

	// Make sure that the volume is managed by ELVM
	hasELVMTag := false
	for _, tag := range logicalVolume.Tags {
		if tag == ELVM_TAG {
			hasELVMTag = true
			break
		}
	}

	if !hasELVMTag {
		return nil, status.Error(
			codes.Aborted,
			"[ERROR] NodePublishVolume Found volume to publish but it is not managed by ELVM. Aborting.",
		)
	}

	// Extract any needed info from the volume
	info, err := getVolumeInfo(logicalVolume)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodePublishVolume Could not get volume info for '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	// Make sure that the volume isn't mounted already
	// Note: The spec says that this should pass
	// https://github.com/container-storage-interface/spec/blob/master/spec.md#nodestagevolume
	for _, mount := range info.MountPoints {
		if mount == request.TargetPath {
			return &csi.NodePublishVolumeResponse{}, nil
		}
	}

	// Get formatting info
	mountInfo := request.VolumeCapability.GetMount()
	var requestedFsType string
	if mountInfo.FsType == "" {
		requestedFsType = server.fsType
	} else {
		requestedFsType = mountInfo.FsType
	}

	// If it isn't formatted correctly, then the staging step has failed
	if info.FsType != requestedFsType {
		return nil, status.Error(
			codes.FailedPrecondition,
			fmt.Sprintf(
				"[ERROR] NodePublishVolume Incorrect FS type for '%s'. Was the staging step skipped? %s != %s",
				request.VolumeId,
				info.FsType,
				requestedFsType,
			),
		)
	}

	// We need to create the directory, so do so here
	if err := os.MkdirAll(request.TargetPath, 0750); err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodePublishVolume Cannot create target directory for %s: %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	// Bind mount the volume
	if err = bindLogicalVolume(logicalVolume, request.StagingTargetPath, request.TargetPath); err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodePublishVolume Could not bind mount volume '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (server *elvmNodeServer) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	// Make sure that we have a volume ID
	if len(request.VolumeId) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodeStageVolume Volume ID must be provided.",
		)
	}

	// Make sure that we have a staging path
	if len(request.StagingTargetPath) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodeStageVolume Staging target path must be provided.",
		)
	}

	// Make sure that we have been given the capability that we require
	if request.VolumeCapability == nil || request.VolumeCapability.AccessMode.Mode != SUPPORTED_CAPABILITY {
		return nil, status.Error(
			codes.FailedPrecondition,
			"[ERROR] NodeStageVolume Only `multi_node_multi_writer` is allowed as a volume capability.",
		)
	}

	// Get all of the logical volumes
	lvs, err := getCurrentLVs(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] NodeStageVolume Could not list logical volumes: %s", err.Error()),
		)
	}

	// Make sure that we have the requested volume
	var logicalVolume *parser.LV
	for _, lv := range lvs {
		if lv.Name == request.VolumeId {
			logicalVolume = lv
			break
		}
	}

	if logicalVolume == nil {
		return nil, status.Error(
			codes.NotFound,
			fmt.Sprintf("[ERROR] NodeStageVolume Could not find requested logical volume: %s", request.VolumeId),
		)
	}

	// Make sure that the volume is managed by ELVM
	hasELVMTag := false
	for _, tag := range logicalVolume.Tags {
		if tag == ELVM_TAG {
			hasELVMTag = true
			break
		}
	}

	if !hasELVMTag {
		return nil, status.Error(
			codes.Aborted,
			"[ERROR] NodeStageVolume Found volume to stage but it is not managed by ELVM. Aborting.",
		)
	}

	// Extract any needed info from the volume
	info, err := getVolumeInfo(logicalVolume)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodeStageVolume Could not get volume info for '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	// Make sure that the volume isn't mounted already
	// Note: The spec says that this should pass
	// https://github.com/container-storage-interface/spec/blob/master/spec.md#nodestagevolume
	for _, mount := range info.MountPoints {
		if mount == request.StagingTargetPath {
			return &csi.NodeStageVolumeResponse{}, nil
		}
	}

	// Get formatting info
	mountInfo := request.VolumeCapability.GetMount()
	var requestedFsType string
	if mountInfo.FsType == "" {
		requestedFsType = server.fsType
	} else {
		requestedFsType = mountInfo.FsType
	}

	log.Println(
		fmt.Sprintf(
			"[INFO] Found logical volume '%s' with fs '%s'",
			logicalVolume,
			info.FsType,
		),
	)

	// Format, if needed
	if info.FsType != requestedFsType {
		log.Println(
			fmt.Sprintf(
				"[INFO] Formatting drive '%s' with fs '%s'",
				logicalVolume.Name,
				requestedFsType,
			),
		)

		err := formatLogicalVolume(logicalVolume, requestedFsType)
		if err != nil {
			return nil, status.Error(
				codes.Internal,
				fmt.Sprintf(
					"[ERROR] NodeStageVolume Could not format volume '%s': %s",
					request.VolumeId,
					err.Error(),
				),
			)
		}
	}

	// Mount the drive to the supplied location
	if err = mountLogicalVolume(logicalVolume, mountInfo.MountFlags, request.StagingTargetPath, requestedFsType); err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodeStageVolume Could not mount volume '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (server *elvmNodeServer) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Make sure that we have a volume ID
	if len(request.VolumeId) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodePublishVolume Volume ID must be provided.",
		)
	}

	// Make sure that we have a target path
	if len(request.TargetPath) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodePublishVolume Target path must be provided.",
		)
	}

	// Get all of the logical volumes
	lvs, err := getCurrentLVs(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] NodePublishVolume Could not list logical volumes: %s", err.Error()),
		)
	}

	// Make sure that we have the requested volume
	var logicalVolume *parser.LV
	for _, lv := range lvs {
		if lv.Name == request.VolumeId {
			logicalVolume = lv
			break
		}
	}

	if logicalVolume == nil {
		return nil, status.Error(
			codes.NotFound,
			fmt.Sprintf("[ERROR] NodeUnpublishVolume Could not find requested logical volume: %s", request.VolumeId),
		)
	}

	// Make sure that the volume is managed by ELVM
	hasELVMTag := false
	for _, tag := range logicalVolume.Tags {
		if tag == ELVM_TAG {
			hasELVMTag = true
			break
		}
	}

	if !hasELVMTag {
		return nil, status.Error(
			codes.Aborted,
			"[ERROR] NodeUnpublishVolume Found volume to publish but it is not managed by ELVM. Aborting.",
		)
	}

	// Extract any needed info from the volume
	info, err := getVolumeInfo(logicalVolume)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodeUnpublishVolume Could not get volume info for '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	// Make sure that the volume isn't mounted already
	// Note: The spec says that this should pass
	// https://github.com/container-storage-interface/spec/blob/master/spec.md#nodestagevolume
	hasMount := false
	for _, mount := range info.MountPoints {
		if mount == request.TargetPath {
			hasMount = true
			break
		}
	}

	// Exit early if not mounted
	if !hasMount {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Unmount the bound volume
	if err = unmountLogicalVolume(request.TargetPath); err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodeUnpublishVolume Could not unbind mount volume '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (server *elvmNodeServer) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	// Make sure that we have a volume ID
	if len(request.VolumeId) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodeStageVolume Volume ID must be provided.",
		)
	}

	// Make sure that we have a staging path
	if len(request.StagingTargetPath) == 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"[ERROR] NodeStageVolume Staging target path must be provided.",
		)
	}

	// Get all of the logical volumes
	lvs, err := getCurrentLVs(ctx, server.volumeGroup)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf("[ERROR] NodeStageVolume Could not list logical volumes: %s", err.Error()),
		)
	}

	// Make sure that we have the requested volume
	var logicalVolume *parser.LV
	for _, lv := range lvs {
		if lv.Name == request.VolumeId {
			logicalVolume = lv
			break
		}
	}

	if logicalVolume == nil {
		return nil, status.Error(
			codes.NotFound,
			fmt.Sprintf("[ERROR] NodeStageVolume Could not find requested logical volume: %s", request.VolumeId),
		)
	}

	// Make sure that the volume is managed by ELVM
	hasELVMTag := false
	for _, tag := range logicalVolume.Tags {
		if tag == ELVM_TAG {
			hasELVMTag = true
			break
		}
	}

	if !hasELVMTag {
		return nil, status.Error(
			codes.Aborted,
			"[ERROR] NodeStageVolume Found volume to stage but it is not managed by ELVM. Aborting.",
		)
	}

	// Extract any needed info from the volume
	info, err := getVolumeInfo(logicalVolume)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodeStageVolume Could not get volume info for '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	// Make sure that the volume is mounted already
	mountFound := false
	for _, mount := range info.MountPoints {
		if mount == request.StagingTargetPath {
			mountFound = true
			break
		}
	}

	// If the volume wasn't mounted, then pass
	if !mountFound {
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// Unmount the drive from the supplied location
	err = unmountLogicalVolume(request.StagingTargetPath)
	if err != nil {
		return nil, status.Error(
			codes.Internal,
			fmt.Sprintf(
				"[ERROR] NodeStageVolume Could not unmount volume '%s': %s",
				request.VolumeId,
				err.Error(),
			),
		)
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}
