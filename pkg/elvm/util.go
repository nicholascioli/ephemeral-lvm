package elvm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"github.com/google/lvmd/commands"
	"github.com/google/lvmd/parser"
)

func getCurrentVG(ctx context.Context, volumeGroup *parser.VG) (*parser.VG, error) {
	// Get all of the volume groups
	vgs, err := commands.ListVG(ctx)
	if err != nil {
		return nil, err
	}

	// Filter out all but the one requested
	var current *parser.VG
	for _, vg := range vgs {
		if vg.UUID == volumeGroup.UUID {
			current = vg
			break
		}
	}

	// Make sure that we found the VG
	if current == nil {
		return nil, errors.New(fmt.Sprintf("Could not find requested VG '%s' in %s", volumeGroup, vgs))
	}

	// Return the result
	return current, nil
}

func getCurrentLVs(ctx context.Context, volumeGroup *parser.VG) ([]*parser.LV, error) {
	lvs, err := commands.ListLV(ctx, volumeGroup.Name)

	// Command is kind of dumb and fails if no volumes in volume group
	// Note: The error is from the parser trying to parse 9 elements from an empty line
	acceptableError := "expected 9 components, got 1"
	if err != nil && err.Error() != acceptableError {
		return nil, err
	}

	return lvs, nil
}

// LVM requires a multiple of the block size of 512, so make sure to do that here
// TODO: Is it always 512? (Seems like it, atl least from lvmd's point of view)
func getCapacity(available uint64, min uint64, max uint64) (uint64, error) {
	// If there wasn't a limit specified or if the limit is larger than what is
	// available, then use the full available space
	if max == 0 || max > available {
		return available, nil
	}

	// Start with the max specified
	capacity := max

	// Align the capacity to 512
	remainder := capacity % 512
	alignedCapacity := capacity + 512 - remainder

	// If the alignment is too large, align down instead
	if alignedCapacity > max || alignedCapacity > available {
		// Make sure that we aren't going to underflow
		if capacity <= remainder {
			return 0, errors.New(
				fmt.Sprintf(
					"Could not create a valid size within constraints. Not enough available space (%d) to meet minimum requirements (%d).",
					available,
					min,
				),
			)
		}

		alignedCapacity = capacity - remainder
	}

	// If the alignment is too small, then fail
	if alignedCapacity < min {
		return 0, errors.New(
			fmt.Sprintf(
				"Could not create a valid size within constraints. Nearest multiples of 512 are outside the requested range [%d, %d].",
				min,
				max,
			),
		)
	}

	return alignedCapacity, nil
}

type LsblkResponse struct {
	BlockDevices []VolumeInfo `json:"blockdevices"`
}

type VolumeInfo struct {
	FsType string `json:"fstype"`
	MountPoints []string `json:"mountpoints"`
}

func getVolumeInfo(logicalVolume *parser.LV) (*VolumeInfo, error) {
	command := "lsblk"

	// Make sure that we have the needed command
	if !isCommandAvailable(command) {
		return nil, errors.New("Could not find command in path: " + command)
	}

	// Set up the needed args
	// Note: LVM escapes - by doubling them
	args := []string {
		"-o", "fstype,mountpoints",
		"--json",
		fmt.Sprintf(
			"/dev/mapper/%s-%s",
			logicalVolume.VGName,
			strings.Replace(logicalVolume.Name, "-", "--", -1),
		),
	}

	// Make sure that the command ran correctly
	cmd := exec.Command(command, args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("%s => %s", string(output), err))
	}

	// Unmarshal the command output into a struct
	var result LsblkResponse
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, errors.New(fmt.Sprintf("Could not unmarshal lsblk => %s", err.Error()))
	}

	return &result.BlockDevices[0], nil
}

func formatLogicalVolume(logicalVolume *parser.LV, fsType string) error {
	command := "mkfs." + fsType

	// Make sure that we have the needed command
	if !isCommandAvailable(command) {
		return errors.New("Could not find command in path: " + command)
	}

	// Get the path to the disk to format
	disk := fmt.Sprintf(
		"/dev/mapper/%s-%s",
		logicalVolume.VGName,
		strings.Replace(logicalVolume.Name, "-", "--", -1),
	)

	// Make sure that the command ran correctly
	cmd := exec.Command(command, disk)
	output, err := cmd.Output()
	if err != nil {
		return errors.New(fmt.Sprintf("%s => %s", string(output), err))
	}

	return nil
}

func isCommandAvailable(name string) bool {
	cmd := exec.Command("/bin/sh", "-c", "command -v " + name)
	err := cmd.Run()

	// The command exists if the exec does not fail
	return err == nil
}

func mountLogicalVolume(logicalVolume *parser.LV, mountFlags []string, target string, fsType string) error {
	source := fmt.Sprintf(
		"/dev/mapper/%s-%s",
		logicalVolume.VGName,
		strings.Replace(logicalVolume.Name, "-", "--", -1),
	)

	return syscall.Mount(source, target, fsType, 0, "")
}

func unmountLogicalVolume(target string) error {
	return syscall.Unmount(target, 0)
}

func bindLogicalVolume(logicalVolume *parser.LV, staging string, target string) error {
	// Bind mount the staging path to the target
	return syscall.Mount(staging, target, "", syscall.MS_BIND, "")
}
