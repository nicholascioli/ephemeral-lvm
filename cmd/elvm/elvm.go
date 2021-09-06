package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	csi "github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/nicholascioli/elvm/pkg/elvm"
)

const (
	defaultDefaultFs = "xfs"

	version = "0.1.0"
)

type logWriter struct {
}

func (writer logWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().UTC().Format("2006-01-02T15:04:05.999Z") + " - " + string(bytes))
}

func killHandler(server *grpc.Server) {
	channel := make(chan os.Signal)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-channel

		log.Println()
		log.Println("Ctrl-C received. Shutting down...")
		server.Stop()
	}()
}

func main() {
	// Set up custom logging
	log.SetFlags(0)
	log.SetOutput(new(logWriter))
	log.Println("Starting ephemeral LVM CSI plugin version", version)

	// Get command arguments
	fsTypeFlag := flag.String("default-fs", defaultDefaultFs, "Default filesystem to use when formatting.")
	nodeIdFlag := flag.String("node-id", "", "ID of the node running the plugin.")
	overwriteSocketFlag := flag.Bool("overwrite-socket", false, "Overwrites the unix socket, if it exists already.")
	unixSocketFlag := flag.String("unix-socket-path", "/tmp/csi.sock", "Path to the listening unix socket.")
	volumeGroupFlag := flag.String("volume-group", "", "The name of the volume group to use.")
	flag.Parse()

	// Make sure that required flags are non-empty
	requireNotEmpty := map[string]string {
		"fsType": *fsTypeFlag,
		"nodeId": *nodeIdFlag,
		"volumeGroup": *volumeGroupFlag,
	}
	for flag, value := range requireNotEmpty {
		if len(value) == 0 {
			log.Fatalln("[ERROR]", flag, "cannot be empty!")
		}
	}

	// Remove the socket file, if specified
	if *overwriteSocketFlag {
		os.Remove(*unixSocketFlag)
	}

	// Make sure that the socket doen't exist already
	if _, err := os.Stat(*unixSocketFlag); err == nil {
		log.Fatalln("[ERROR] Socket file path already exists!", *unixSocketFlag)
	}

	// Print out the current configuration
	log.Println("Got the following configuration...")
	log.Println("\tNode ID:", *nodeIdFlag)
	log.Println("\tUnix Socket path:", *unixSocketFlag)
	log.Println("\tVolume Group:", *volumeGroupFlag)

	// Setup socket listener
	socket, err := net.Listen("unix", *unixSocketFlag)
	if err != nil {
		log.Fatalf("[ERROR] Failed to set up unix socket listener:", err)
	}

	// Delete the socket listener when we finish
	defer os.Remove(*unixSocketFlag)

	// Set up a gRPC middlewear for logging all requests
	requestInterceptor := func(ctx context.Context, request interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Log the access
		log.Println("[ACCESS]", info.FullMethod)

		// Run the method
		resp, err := handler(ctx, request)

		// Log any errors
		if err != nil {
			log.Fatalln("[ACCESS ERR]", info.FullMethod, err.Error())
		}

		// Pass the response on
		return resp, err
	}

	// Setup server
	server := grpc.NewServer(grpc.UnaryInterceptor(requestInterceptor))
	elvmServer := elvm.NewELVMServer()

	// Register the CSI endpoints
	identity, controller, node := elvmServer.GetCSIEndpoints(&elvm.ELVMArgs{
		FsType: *fsTypeFlag,
		NodeId: *nodeIdFlag,
		VolumeGroup: *volumeGroupFlag,
	})

	csi.RegisterIdentityServer(server, identity)
	csi.RegisterControllerServer(server, controller)
	csi.RegisterNodeServer(server, node)

	// Set up kill handler
	killHandler(server)

	// Start serving
	if err := server.Serve(socket); err != nil {
		log.Fatalf("[ERROR] Failed to serve =>", err)
	}
}
